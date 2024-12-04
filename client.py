import socket
import json
import cmd
import time

# Cluster configurations
CLUSTER_A = ['node2-a1', 'node2-a2', 'node2-a3']
CLUSTER_B = ['node3-b1', 'node3-b2', 'node3-b3']

# Updated node configuration with replicas
NODES = {
    # Coordinator
    'node1': {'ip': '10.128.0.3', 'port': 5001, 'role': 'coordinator'},
    
    # Account A cluster
    'node2-a1': {'ip': '10.128.0.5', 'port': 5002, 'role': 'primary-a', 'cluster': 'a'},
    'node2-a2': {'ip': '10.128.0.7', 'port': 5004, 'role': 'replica-a', 'cluster': 'a'},
    'node2-a3': {'ip': '10.128.0.8', 'port': 5005, 'role': 'replica-a', 'cluster': 'a'},
    
    # Account B cluster
    'node3-b1': {'ip': '10.128.0.6', 'port': 5003, 'role': 'primary-b', 'cluster': 'b'},
    'node3-b2': {'ip': '10.128.0.9', 'port': 5006, 'role': 'replica-b', 'cluster': 'b'},
    'node3-b3': {'ip': '10.128.0.10', 'port': 5008, 'role': 'replica-b', 'cluster': 'b'}
}

# For local testing
#LOCAL_NODES = {name: {'ip': 'localhost', 'port': node['port'], 'role': node['role'], 
 #              'cluster': node.get('cluster')} for name, node in NODES.items()}

"""
This code initializes the client class. 
It runs a continuous command line loop and respondes to specific inputs that are then sent to all
the cluster nodes through RPC.
"""
class RaftClient(cmd.Cmd):
    intro = 'Welcome to the Raft cluster client. Type help or ? to list commands.\n'
    prompt = '(raft) '


    def do_transfer(self, arg):
        'Initiate a transfer transaction: transfer'
        print("\nInitiating transfer transaction...")
        
        # Always send to coordinator first
        coordinator_info = NODES['node1']
        response = self.send_rpc(
            coordinator_info['ip'],
            coordinator_info['port'],
            'TransactionRequest',
            {'type': 'transfer', 'amount': 100},
            timeout=5
        )
        
        if response:
            if response.get('redirect'):
                print(f"Redirecting to coordinator...")
                return
                
            if response.get('success'):
                print(f"Transaction completed successfully!")
                print(f"Status: {response.get('status')}")
                return
            else:
                print(f"Transaction failed: {response.get('error', 'Unknown error')}")
                return
        else:
            print("Transaction failed - coordinator not responding")

    def do_bonus(self, arg):
        'Initiate a bonus transaction: bonus'
        print("Initiating bonus transaction...")
        self.contact_nodes('TransactionRequest', {
            'type': 'bonus'
        })

    def do_setup(self, arg):
        'Setup test scenario: setup <scenario> [failure_mode]'
        args = arg.split()
        if not args:
            print("Error: Scenario required (A, B, or C)")
            return
            
        scenario = args[0].upper()  # Make sure it's uppercase
        failure_mode = args[1] if len(args) > 1 else None
        
        if scenario not in ['A', 'B', 'C']:
            print("Invalid scenario. Choose A, B, or C")
            return
        
        # Validate failure modes
        valid_failure_modes = ['before_prepare', 'after_prepare', 'coordinator_after_commit']
        if failure_mode and failure_mode not in valid_failure_modes:
            print(f"Invalid failure mode. Choose from: {', '.join(valid_failure_modes)}")
            return
            
        print(f"Setting up scenario {scenario}" + 
            (f" with failure mode {failure_mode}" if failure_mode else ""))
        
        self.contact_nodes('SetupScenario', {
            'scenario': scenario,
            'failure_mode': failure_mode
        })

        def handle_transaction_response(self, response):
            """Handle transaction response from coordinator"""
            if response.get('redirect'):
                print(f"Redirecting to coordinator...")
                return
                
            if response.get('success'):
                print(f"Transaction completed successfully!")
                print(f"Status: {response.get('status')}")
                return
            else:
                print(f"Transaction failed: {response.get('error', 'Unknown error')}")

    """
    This function sends a RPC request to a specific node.
    """
    def send_rpc(self, ip, port, rpc_type, data, timeout=5):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(timeout)
                s.connect((ip, port))
                message = json.dumps({'rpc_type': rpc_type, 'data': data})
                s.sendall(message.encode())
                response = s.recv(4096).decode()
                return json.loads(response)
        except socket.timeout:
            print(f"Timeout communicating with node {ip}:{port}")
            return None
        except Exception as e:
            print(f"Error communicating with node {ip}:{port}: {str(e)}")
            return None

  
    def contact_nodes(self, rpc_type, data):
        """
        This function tries to send RPC to nodes until successful. It is used to determine the current leader.
        """
        tried_nodes = set()
        MAX_RETRIES = 3
        retries = 0

        while retries < MAX_RETRIES:
            for node_name in NODES:
                if node_name in tried_nodes:
                    continue
                node_info = NODES[node_name]
                response = self.send_rpc(node_info['ip'], node_info['port'], rpc_type, data)
                
                if response:
                    if response.get('redirect'):
                        leader_name = response['leader_name']
                        if leader_name and leader_name in NODES:
                            tried_nodes.add(node_name)
                            print(f"Redirected to leader: {leader_name}")
                            leader_info = NODES[leader_name]
                            response = self.send_rpc(leader_info['ip'], leader_info['port'], rpc_type, data)
                            if response and response.get('success'):
                                print("Operation successful")
                                return
                            else:
                                print("Operation failed at leader")
                        else:
                            print("Leader unknown. Retrying...")
                            tried_nodes.add(node_name)
                    elif response.get('success'):
                        print("Operation successful")
                        return
                    elif response.get('status'):
                        print(f"Status: {response['status']}")
                        return
                    else:
                        print("Operation failed")
                else:
                    print(f"No response from {node_name}")
                    tried_nodes.add(node_name)
                    
            retries += 1
            if len(tried_nodes) == len(NODES):
                print("Unable to complete operation. No nodes available.")
                return
        print("Max retries reached. Operation failed.")

    """
    This function submits a value to the cluster. It is used to update the current leader with a new value.
    """
    def do_submit(self, arg):
        'Submit a value to the cluster: submit <value>'
        if not arg:
            print("Error: Value required")
            print("Usage: submit <value>")
            return
        print(f"Submitting value: {arg}")
        self.contact_nodes('SubmitValue', {'value': arg})

    """
    This function finds the current leader in the cluster. It is used to determine the current leader. 
    It does so by contacting all nodes in the cluster and checking if they are the leader.
    """
    def find_current_leader(self):
        """Find the current leader in the cluster"""
        for node_name, node_info in NODES.items():
            try:
                response = self.send_rpc(
                    node_info['ip'],
                    node_info['port'],
                    'GetStatus',
                    {}
                )
                if response and response.get('is_leader'):
                    return node_name, node_info
                elif response and response.get('leader_name'):
                    leader_name = response['leader_name']
                    if leader_name in NODES:
                        return leader_name, NODES[leader_name]
            except Exception:
                continue
        return None, None

    """
    This function triggers a leader change in the cluster.
    """
    def do_leader(self, arg):
        'Trigger a leader change in the cluster'
        # First find current leader
        leader_name, leader_info = self.find_current_leader()
        if not leader_name:
            print("No leader found in the cluster")
            return

        print(f"Current leader is {leader_name}, attempting leader change...")
        response = self.send_rpc(
            leader_info['ip'],
            leader_info['port'],
            'TriggerLeaderChange',
            {}
        )
        
        if response and response.get('status') == 'Leader stepping down':
            print(f"Leader {leader_name} is stepping down")
            print("Waiting for new leader election...")
        else:
            print("Leader change failed")


    """
    This function gets detailed status of all nodes in the cluster and displays it.
    """
    def do_status(self, arg):
        'Get detailed status of all nodes in the cluster'
        print("\nCluster Status:")
        print("--------------")
        
        for node_name, node_info in NODES.items():
            try:
                response = self.send_rpc(
                    node_info['ip'],
                    node_info['port'],
                    'GetStatus',
                    {}
                )
                
                if response:
                    state = response.get('state', 'UNKNOWN')
                    role = node_info['role']
                    cluster = node_info.get('cluster', 'coordinator')
                    
                    print(f"\n{node_name}:")
                    print(f"  Role: {role}")
                    print(f"  Cluster: {cluster}")
                    print(f"  State: {state}")
                    
                    # Get balance for nodes with accounts
                    if 'cluster' in node_info:
                        balance_response = self.send_rpc(
                            node_info['ip'],
                            node_info['port'],
                            'GetBalance',
                            {}
                        )
                        if balance_response and 'balance' in balance_response:
                            print(f"  Balance: {balance_response['balance']}")
                else:
                    print(f"\n{node_name}: UNREACHABLE")
            except Exception as e:
                print(f"\n{node_name}: UNREACHABLE ({str(e)})")
        print()

    """
    This function displays a list of available commands and detailed help for specific commands.
    """
    def do_help(self, arg):
        'List available commands with "help" or detailed help with "help cmd".'
        print("\nAvailable commands:")
        print("  scenario <A|B|C> [failure_mode] - Set up a specific test scenario")
        print("  transfer                        - Initiate a transfer transaction")
        print("  bonus                           - Initiate a bonus transaction")
        print("  setup <scenario> [failure_mode] - Setup test scenario")
        print("  submit <value>                  - Submit a value to the cluster")
        print("  leader                          - Trigger a leader change")
        print("  status                          - Show status of all nodes")
        print("  quit/exit                       - Exit the client")
        print("\nFor more details on a specific command, type: help <command>")

    def do_quit(self, arg):
        'Exit the client'
        print("Goodbye!")
        return True

    def do_exit(self, arg):
        'Exit the client'
        return self.do_quit(arg)

    def default(self, line):
        print(f"Unknown command: {line}")
        print("Type 'help' or '?' to see available commands")


"""
This is the main function that runs the client.
It initializes the RaftClient object and starts the command loop.
The loop continues until the user enters 'exit' or 'quit'.
"""
if __name__ == '__main__':
    try:
        RaftClient().cmdloop()
    except KeyboardInterrupt:
        print("\nGoodbye!")