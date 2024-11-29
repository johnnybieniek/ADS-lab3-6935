# node.py
import socket
import threading
import time
import json
import sys
import random

# Hardcoding the nodes IP addresses and ports for ease of use
# NODES = {
#     'node1': {'ip': '10.128.0.3', 'port': 5001},
#     'node2': {'ip': '10.128.0.5', 'port': 5002},
#     'node3': {'ip': '10.128.0.6', 'port': 5003},
# }
NODES = {
    'node1': {'ip': 'localhost', 'port': 5001},
    'node2': {'ip': 'localhost', 'port': 5002},
    'node3': {'ip': 'localhost', 'port': 5003},
}

# Timeout settings (in seconds)
ELECTION_TIMEOUT = (2, 4)  # Range for random election timeout to make it more realistic
HEARTBEAT_INTERVAL = 0.5  # Interval for leader to send heartbeats


"""
Initialize the Node class for each node. 
Some of the key attributes are:
- name: The name of the node
- ip: The IP address of the node
- port: The port number of the node
- state: The current state of the node (Follower, Candidate, Leader)
- current_term: The current term of the node
- voted_for: The node that received the most votes in the current term
- log: The log of the node
- commit_index: The index of the last committed entry in the log
- last_applied: The index of the last applied entry in the log
- next_index: The next index to be replicated to each follower
- match_index: The index of the last entry that has been replicated to each follower
- leader_id: The ID of the current leader
- election_timer: The timer for the election timeout
- heartbeat_timer: The timer for the heartbeat interval
- server_socket: The socket for the node to listen for incoming connections
- running: A flag to indicate whether the node is running or not
- lock: A lock to ensure thread safety
"""
class Node:
    def __init__(self, name, scenario='A', failure_mode=None):
        self.name = name
        self.ip = NODES[self.name]['ip']
        self.port = NODES[self.name]['port']
        self.state = 'Follower'
        self.current_term = 0
        self.voted_for = None
        self.log = [] 
        self.commit_index = -1
        self.last_applied = -1
        self.next_index = {}
        self.match_index = {}
        self.leader_id = None
        self.election_timer = None
        self.heartbeat_timer = None
        self.server_socket = None
        self.running = True
        self.lock = threading.Lock()

        self.failure_mode = failure_mode
        self.scenario = scenario
        self.has_failed = False
        
        # Creating the log file
        self.log_filename = f"CISC6935-{self.name}"
        open(self.log_filename, 'w').close()


        # adding some 2PC specific attributes and hardcoding the accounts to specific nodes

        self.is_coordinator = (name == 'node1')  # Node1 is coordinator
        self.account_file = None

        if name == 'node2':
            self.account_file = 'account_A.txt'
            self.account_name = 'A'
        elif name == 'node3':
            self.account_file = 'account_B.txt'
            self.account_name = 'B'
        
        # 2PC Transaction State
        self.transaction_state = {
            'status': None,  # 'preparing', 'committing', 'aborting'
            'transaction_id': None,
            'participants_ready': set(),
            'participants_committed': set(),
            'transaction_log': [],
            'current_transaction': None
        }
        
        # Initialize account if this node manages one
        if self.account_file:
            self.initialize_account(scenario)



    def initialize_account(self, scenario='A'):
        """Initialize account file with balance based on scenario"""
        scenarios = {
            'A': {'A': 200.0, 'B': 300.0},
            'B': {'A': 90.0, 'B': 50.0},
            'C': {'A': 200.0, 'B': 300.0}
        }
        print(f"[{self.name}] Scenarios: {scenarios}, in the initialize_account function")
        if not self.account_file or not self.account_name:
            return
            
        initial_balance = scenarios[scenario][self.account_name]
        print(f"[{self.name}] Initializing account {self.account_name} with balance: {initial_balance}")
        
        try:
            with open(self.account_file, 'w') as f:
                f.write(str(initial_balance))
        except Exception as e:
            print(f"[{self.name}] Error writing to account file: {str(e)}")

    def get_balance(self):
        """Get current account balance"""
        if not self.account_file:
            return None
        with open(self.account_file, 'r') as f:
            return float(f.read().strip())
        
    def simulate_failure(self):
        """Simulate node failure based on failure mode"""
        if self.failure_mode:
            print(f"[{self.name}] Simulating failure: {self.failure_mode}")
            time.sleep(10)  # Simulate crash with sleep
            self.has_failed = True

    def update_balance(self, new_balance):
        """Update account balance"""
        if not self.account_file:
            return False
        with open(self.account_file, 'w') as f:
            f.write(str(new_balance))
        return True
    
    def simulate_failure(self):
        """Simulate node failure with sleep"""
        print(f"[{self.name}] Simulating node failure...")
        time.sleep(10)  # 10 second delay to simulate crash
        self.has_failed = True

    def send_prepare_to_participants(self, transaction_id, transaction_data):
        """Coordinator sends prepare messages to all participants"""
        if not self.is_coordinator:
            return False
            
        prepare_data = {
            'transaction_id': transaction_id,
            'transaction': transaction_data
        }
        
        # For bonus transactions, we need to get A's response first to calculate B's bonus
        if transaction_data['type'] == 'bonus':
            # First, get node A's response to calculate bonus
            node_a_response = self.send_rpc(
                NODES['node2']['ip'],
                NODES['node2']['port'],
                'Prepare',
                prepare_data
            )
            
            if node_a_response and node_a_response.get('success'):
                # Get the bonus amount calculated by A
                bonus_amount = node_a_response.get('bonus_amount')
                # Update transaction data with bonus amount for B
                prepare_data['transaction']['bonus_amount'] = bonus_amount
                
                # Now send to node B with the bonus amount
                node_b_response = self.send_rpc(
                    NODES['node3']['ip'],
                    NODES['node3']['port'],
                    'Prepare',
                    prepare_data
                )
                
                if node_b_response and node_b_response.get('success'):
                    self.transaction_state['participants_ready'].add('node2')
                    self.transaction_state['participants_ready'].add('node3')
                    return True
        else:
            # Handle non-bonus transactions as before
            success_responses = 0
            for node_name, node_info in NODES.items():
                if node_name != self.name:
                    response = self.send_rpc(
                        node_info['ip'],
                        node_info['port'],
                        'Prepare',
                        prepare_data
                    )
                    if response and response.get('success'):
                        success_responses += 1
                        self.transaction_state['participants_ready'].add(node_name)
                        
            return success_responses == len(NODES) - 1

        return False
    
    def send_commit_to_participants(self, transaction_id):
        """Send commit message to all participants"""
        if not self.is_coordinator:
            return False
            
        success_responses = 0
        for node_name in self.transaction_state['participants_ready']:
            node_info = NODES[node_name]
            response = self.send_rpc(
                node_info['ip'],
                node_info['port'],
                'Commit',
                {'transaction_id': transaction_id}
            )
            if response and response.get('success'):
                success_responses += 1
                
        return success_responses == len(self.transaction_state['participants_ready'])

    def send_abort_to_participants(self, transaction_id):
        """Send abort message to all participants"""
        if not self.is_coordinator:
            return False
            
        for node_name, node_info in NODES.items():
            if node_name != self.name:
                self.send_rpc(
                    node_info['ip'],
                    node_info['port'],
                    'Abort',
                    {'transaction_id': transaction_id}
                )
        return True

    def handle_setup_scenario(self, data):
        """Handle scenario setup request"""
        try:
            scenario = data.get('scenario')
            failure_mode = data.get('failure_mode', None)
            
            print(f"[{self.name}] Setting up scenario {scenario} with failure mode: {failure_mode}")
            
            if scenario not in ['A', 'B', 'C']:
                print(f"[{self.name}] Invalid scenario: {scenario}")
                return {'success': False, 'error': 'Invalid scenario'}

            # Define scenario balances
            scenarios = {
                'A': {'A': 200.0, 'B': 300.0},
                'B': {'A': 90.0, 'B': 50.0},
                'C': {'A': 200.0, 'B': 300.0}
            }

            # Update state
            self.scenario = scenario
            self.failure_mode = failure_mode
            self.has_failed = False

            # If this is the coordinator (node1), send setup to other nodes first
            if self.is_coordinator:
                print(f"[{self.name}] Coordinator sending setup to other nodes")
                
                # Send to node2 (Account A)
                response_A = self.send_rpc(
                    NODES['node2']['ip'],
                    NODES['node2']['port'],
                    'SetupScenario',
                    data
                )
                
                # Send to node3 (Account B)
                response_B = self.send_rpc(
                    NODES['node3']['ip'],
                    NODES['node3']['port'],
                    'SetupScenario',
                    data
                )
                
                if not response_A or not response_B:
                    return {'success': False, 'error': 'Failed to setup all nodes'}
            
            # Directly update balance if this node manages an account
            if self.account_name:
                new_balance = scenarios[scenario][self.account_name]
                success = self.update_balance(new_balance)
                if success:
                    print(f"[{self.name}] Successfully updated balance to {new_balance}")
                else:
                    print(f"[{self.name}] Failed to update balance")
                    return {'success': False, 'error': 'Failed to update balance'}
            
            return {'success': True}
                
        except Exception as e:
            print(f"[{self.name}] Error in setup: {str(e)}")
            return {'success': False, 'error': str(e)}
    """
    The main function to start the node. It creates a server thread and starts the election process.
    It also handles the heartbeats and appends new entries to the log.
    """
    def start(self):
        print(f"[{self.name}] Starting node...")
        server_thread = threading.Thread(target=self.run_server)
        server_thread.daemon = True
        server_thread.start()
        
        self.reset_election_timer()

        while self.running:
            with self.lock:
                current_state = self.state
                time_until_next_election = self.election_timer

            if current_state == 'Leader':
                self.send_heartbeats()
                time.sleep(HEARTBEAT_INTERVAL)
            else:
                if time_until_next_election <= 0:
                    with self.lock:
                        if self.state != 'Leader': 
                            self.start_election()
                time.sleep(0.1)
                with self.lock:
                    self.election_timer -= 0.1

    """
    This function is the main server loop. It listens for incoming client connections and handles them.
    """
    def run_server(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.ip, self.port))
        self.server_socket.listen(5)
        print(f"[{self.name}] Listening for client connections at {self.ip}:{self.port}")

        while self.running:
            try:
                client_socket, addr = self.server_socket.accept()
                client_thread = threading.Thread(
                    target=self.handle_client_connection,
                    args=(client_socket,)
                )
                client_thread.daemon = True
                client_thread.start()
            except Exception as e:
                if self.running:
                    print(f"[{self.name}] Server error: {e}")
    
    """
    This function handles incoming client connections. It extracts the RPC type and data from the request,
    and then calls the appropriate function to handle the request.
    """
    def handle_client_connection(self, client_socket):
        try:
            data = client_socket.recv(4096).decode()
            if data:
                request = json.loads(data)
                rpc_type = request['rpc_type']
                response = {}

                with self.lock:
                    # Keep existing RPC handlers
                    if rpc_type == 'RequestVote':
                        response = self.handle_request_vote(request['data'])
                    elif rpc_type == 'AppendEntries':
                        response = self.handle_append_entries(request['data'])
                    # Add new 2PC RPC handlers
                    elif rpc_type == 'SetupScenario':  # Add this line
                        print(f"[{self.name}] Handling setup scenario request")
                        response = self.handle_setup_scenario(request['data'])  # Add this line
                    elif rpc_type == 'Prepare':
                        response = self.handle_prepare(request['data'])
                    elif rpc_type == 'Commit':
                        response = self.handle_commit(request['data'])
                    elif rpc_type == 'Abort':
                        response = self.handle_abort(request['data'])
                    elif rpc_type == 'TransactionRequest':
                        response = self.handle_transaction_request(request['data'])
                    # Keep other existing handlers
                    elif rpc_type == 'GetStatus':
                        response = self.handle_get_status()
                    else:
                        response = {'error': 'Unknown RPC type'}

                client_socket.sendall(json.dumps(response).encode())
        except Exception as e:
            print(f"[{self.name}] Error handling client connection: {e}")
        finally:
            client_socket.close()

    # Reset the election timer to a random value within the specified range
    def reset_election_timer(self):
        self.election_timer = random.uniform(*ELECTION_TIMEOUT)

    def handle_transaction_request(self, data):
        """Handle incoming transaction request from client"""
        if not self.is_coordinator:
            print(f"[{self.name}] Not coordinator, redirecting to node1")
            return {
                'success': False,
                'error': 'Not the coordinator',
                'redirect': True,
                'leader_name': 'node1'
            }

        transaction_id = str(time.time())
        transaction_type = data.get('type')
        
        print(f"\n[{self.name}] COORDINATOR: Starting new transaction")
        print(f"[{self.name}] COORDINATOR: Transaction ID: {transaction_id}")
        print(f"[{self.name}] COORDINATOR: Transaction type: {transaction_type}")
        
        if transaction_type not in ['transfer', 'bonus']:
            return {
                'success': False,
                'error': 'Invalid transaction type'
            }

        self.transaction_state.update({
            'transaction_id': transaction_id,
            'status': 'preparing',
            'current_transaction': data,
            'participants_ready': set(),
        })
        
        print(f"[{self.name}] COORDINATOR: Sending PREPARE to all participants")
        prepare_success = self.send_prepare_to_participants(transaction_id, data)
        
        if prepare_success:
            print(f"[{self.name}] COORDINATOR: All participants ready, initiating COMMIT")
            commit_success = self.send_commit_to_participants(transaction_id)
            if commit_success:
                print(f"[{self.name}] COORDINATOR: Transaction completed successfully")
                return {
                    'success': True,
                    'status': 'committed',
                    'transaction_id': transaction_id,
                    'message': 'Transaction completed successfully'
                }
            else:
                print(f"[{self.name}] COORDINATOR: Commit failed, initiating ABORT")
                self.send_abort_to_participants(transaction_id)
                return {
                    'success': False,
                    'status': 'aborted',
                    'error': 'Commit failed'
                }
        else:
            print(f"[{self.name}] COORDINATOR: Prepare failed, initiating ABORT")
            self.send_abort_to_participants(transaction_id)
            return {
                'success': False,
                'status': 'aborted',
                'error': 'Prepare failed'
            }

    def handle_prepare(self, data):
        """
        Handle prepare request from coordinator for 2PC protocol.
        Includes failure simulation for testing scenarios.
        """
        try:
            # First, check if we should simulate failure BEFORE prepare
            if self.failure_mode == 'before_prepare' and not self.has_failed:
                print(f"[{self.name}] Simulating failure before prepare...")
                self.has_failed = True
                self.simulate_failure()
                return None  # Return None to simulate no response due to crash
                
            # Normal prepare handling continues if no failure...
            if self.is_coordinator:
                print(f"[{self.name}] Error: Coordinator cannot handle prepare phase")
                return {
                    'success': False, 
                    'error': 'Coordinator cannot handle prepare phase'
                }

            transaction_id = data.get('transaction_id')
            transaction = data.get('transaction')

            print(f"\n[{self.name}] Received PREPARE for transaction {transaction_id}")
            current_balance = self.get_balance()
            print(f"[{self.name}] Current balance: {current_balance}")

            # Transaction validation based on type
            transaction_type = transaction.get('type')
            can_process = False

            if transaction_type == 'transfer':
                if self.account_name == 'A':
                    can_process = current_balance >= 100
                    print(f"[{self.name}] Transfer validation: {'Sufficient' if can_process else 'Insufficient'} funds")
                else:  # Account B
                    can_process = True
                    print(f"[{self.name}] Transfer validation: Can receive")

            elif transaction_type == 'bonus':
                can_process = True
                if self.account_name == 'A':
                    bonus_amount = current_balance * 0.2
                    print(f"[{self.name}] Calculating bonus: {bonus_amount}")
                else:  # Account B
                    bonus_amount = transaction.get('bonus_amount', 0)
                    print(f"[{self.name}] Will receive bonus: {bonus_amount}")

            if can_process:
                self.transaction_state.update({
                    'status': 'prepared',
                    'transaction_id': transaction_id,
                    'current_transaction': transaction,
                    'prepare_timestamp': time.time()
                })

                # Check if we should simulate failure AFTER prepare
                if self.failure_mode == 'after_prepare' and not self.has_failed:
                    print(f"[{self.name}] Sending READY response before simulated failure")
                    response = {
                        'success': True,
                        'status': 'ready',
                        'node': self.name,
                        'transaction_id': transaction_id
                    }
                    # Add bonus amount to response if relevant
                    if transaction_type == 'bonus' and self.account_name == 'A':
                        response['bonus_amount'] = bonus_amount
                    
                    print(f"[{self.name}] Simulating failure after prepare...")
                    self.simulate_failure()
                    return response

                print(f"[{self.name}] Sending READY response")
                return {
                    'success': True,
                    'status': 'ready',
                    'node': self.name,
                    'transaction_id': transaction_id,
                    'bonus_amount': bonus_amount if transaction_type == 'bonus' and self.account_name == 'A' else None
                }
            else:
                print(f"[{self.name}] Sending ABORT response")
                return {
                    'success': False,
                    'status': 'abort',
                    'node': self.name,
                    'transaction_id': transaction_id,
                    'reason': 'Transaction validation failed'
                }

        except Exception as e:
            print(f"[{self.name}] Error in prepare phase: {str(e)}")
            return {
                'success': False,
                'error': f'Internal error during prepare phase: {str(e)}'
            }
    

    def handle_commit(self, data):
        """
        Handle commit request from coordinator.
        Executes the prepared transaction and updates account balances.
        
        Args:
            data (dict): Contains transaction_id and any additional transaction data
            
        Returns:
            dict: Response indicating success or failure of commit operation
        """
        try:
            transaction_id = data.get('transaction_id')
            print(f"\n[{self.name}] Received COMMIT for transaction {transaction_id}")
            
            # Verify transaction state
            if not self.transaction_state:
                print(f"[{self.name}] Error: No transaction state found")
                return {'success': False, 'error': 'No transaction state found'}
                
            if self.transaction_state.get('status') != 'prepared':
                print(f"[{self.name}] Error: Not in prepared state")
                print(f"Current state: {self.transaction_state.get('status', 'unknown')}")
                return {'success': False, 'error': 'Not prepared'}
                
            # Get the prepared transaction
            transaction = self.transaction_state.get('current_transaction')
            if not transaction:
                print(f"[{self.name}] Error: No transaction found")
                return {'success': False, 'error': 'No transaction found'}
            
            # Get current balance and execute transaction
            current_balance = self.get_balance()
            print(f"[{self.name}] Executing transaction on balance: {current_balance}")
            
            # Handle different transaction types
            transaction_type = transaction.get('type')
            if transaction_type == 'transfer':
                if self.account_name == 'A':
                    new_balance = current_balance - 100
                else:  # Account B
                    new_balance = current_balance + 100
            elif transaction_type == 'bonus':
                if self.account_name == 'A':
                    bonus_amount = self.transaction_state.get('bonus_amount', current_balance * 0.2)
                    new_balance = current_balance + bonus_amount
                    print(f"[{self.name}] Adding bonus of {bonus_amount}")
                else:  # Account B
                    bonus_amount = transaction.get('bonus_amount', 0)
                    new_balance = current_balance + bonus_amount
                    print(f"[{self.name}] Adding bonus of {bonus_amount}")
            else:
                print(f"[{self.name}] Error: Invalid transaction type {transaction_type}")
                return {'success': False, 'error': 'Invalid transaction type'}
            
            # Update balance and verify
            try:
                success = self.update_balance(new_balance)
                if not success:
                    print(f"[{self.name}] Failed to update balance")
                    return {'success': False, 'error': 'Failed to update balance'}
            except Exception as e:
                print(f"[{self.name}] Error updating balance: {str(e)}")
                return {'success': False, 'error': f'Balance update failed: {str(e)}'}
            
            # Update transaction state
            self.transaction_state['status'] = 'committed'
            print(f"[{self.name}] Transaction committed. New balance: {new_balance}")
            
            return {
                'success': True,
                'status': 'committed',
                'transaction_id': transaction_id,
                'new_balance': new_balance
            }
                
        except Exception as e:
            print(f"[{self.name}] Error in commit phase: {str(e)}")
            return {
                'success': False,
                'error': f'Internal error during commit phase: {str(e)}'
            }

    def handle_abort(self, data):
        """Handle abort request from coordinator"""
        self.transaction_state['status'] = 'aborted'
        self.transaction_state['current_transaction'] = None
        return {'success': True}
    

    def verify_transaction(self, transaction):
        """Verify if transaction is possible"""
        if not self.account_file:
            return False
            
        current_balance = self.get_balance()
        transaction_type = transaction['type']
        
        if transaction_type == 'transfer':
            if self.account_name == 'A':
                return current_balance >= 100
            return True
        elif transaction_type == 'bonus':
            return True
        
        return False

    def execute_transaction(self, transaction):
        """Execute the transaction"""
        if not self.account_file:
            return False
            
        try:
            current_balance = self.get_balance()
            transaction_type = transaction['type']
            
            if transaction_type == 'transfer':
                if self.account_name == 'A':
                    new_balance = current_balance - 100
                else:  # Account B
                    new_balance = current_balance + 100
            elif transaction_type == 'bonus':
                if self.account_name == 'A':
                    bonus = current_balance * 0.2
                    new_balance = current_balance + bonus
                else:  # Account B
                    bonus = transaction['bonus_amount']  # Passed from A's calculation
                    new_balance = current_balance + bonus
                    
            self.update_balance(new_balance)
            return True
        except Exception as e:
            print(f"[{self.name}] Error executing transaction: {e}")
            return False

    """
    This function handles the RequestVote RPC. It checks if the term is higher than the current term,
    and if the candidate has a higher term or the same term but a higher log index. If so, it votes for the candidate.
    """
    def handle_request_vote(self, data):
        candidate_term = data['term']
        candidate_id = data['candidate_name']
        candidate_last_log_index = data['last_log_index']
        candidate_last_log_term = data['last_log_term']

        if candidate_term < self.current_term:
            return {
                'term': self.current_term,
                'vote_granted': False
            }

        if candidate_term > self.current_term:
            self.current_term = candidate_term
            self.state = 'Follower'
            self.voted_for = None
            self.leader_id = None

        # Check if we can vote for this candidate
        can_vote = (self.voted_for is None or self.voted_for == candidate_id)
        
        # Check if candidate's log is at least as up-to-date as ours
        last_log_index = len(self.log) - 1
        last_log_term = self.log[last_log_index]['term'] if self.log else 0

        log_is_up_to_date = (
            candidate_last_log_term > last_log_term or
            (candidate_last_log_term == last_log_term and
             candidate_last_log_index >= last_log_index)
        )

        if can_vote and log_is_up_to_date:
            self.voted_for = candidate_id
            self.reset_election_timer()
            print(f"[{self.name}] Voted for {candidate_id} in term {self.current_term}")
            return {
                'term': self.current_term,
                'vote_granted': True
            }

        return {
            'term': self.current_term,
            'vote_granted': False
        }
    
    """
    This function handles the AppendEntries RPC. It checks if the term is higher than the current term,
    and if the leader has a higher term or the same term but a higher log index. If so, it appends the entries to the log.
    """
    def handle_append_entries(self, data):
        leader_term = data['term']
        leader_id = data['leader_name']
        prev_log_index = data['prev_log_index']
        prev_log_term = data['prev_log_term']
        entries = data['entries']
        leader_commit = data['leader_commit']

        # Reply false if term < currentTerm
        if leader_term < self.current_term:
            return {'term': self.current_term, 'success': False}

        # Update term if needed
        if leader_term > self.current_term:
            self.current_term = leader_term
            self.voted_for = None

        # Reset election timer and update leader
        self.reset_election_timer()
        self.state = 'Follower'
        self.leader_id = leader_id

        # Check log consistency
        if prev_log_index >= len(self.log):
            return {'term': self.current_term, 'success': False}
        
        if prev_log_index >= 0 and (
            prev_log_index >= len(self.log) or
            self.log[prev_log_index]['term'] != prev_log_term
        ):
            return {'term': self.current_term, 'success': False}

        # Process new entries
        if entries:
            # Delete conflicting entries
            self.log = self.log[:prev_log_index + 1]
            # Append new entries
            self.log.extend(entries)
            #print(f"[{self.name}] Appended {len(entries)} entries to log")

        # Update commit index
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, len(self.log) - 1)
            self.apply_committed_entries()

        return {'term': self.current_term, 'success': True}

    # This function applies the committed entries to the state machine
    def apply_committed_entries(self):
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied]
            self.apply_entry_to_state_machine(entry)

    # This function applies a single entry to the state machine
    def apply_entry_to_state_machine(self, entry):
        with open(self.log_filename, 'a') as f:
            f.write(f"{entry}\n")
        print(f"[{self.name}] Applied value to the log: {entry['value']}")

    """
    This function starts the election for the next term. It resets the election timer and   
    sends RequestVote RPCs to all other nodes. When a majority of nodes have voted for the candidate,
    it becomes the leader and starts the heartbeat process.
    """
    def start_election(self):
        self.state = 'Candidate'
        self.current_term += 1
        self.voted_for = self.name
        self.leader_id = None
        votes_received = 1  # Vote for self

        print(f"[{self.name}] Starting election for term {self.current_term}")
        self.reset_election_timer()

        # Prepare RequestVote arguments
        last_log_index = len(self.log) - 1
        last_log_term = self.log[last_log_index]['term'] if self.log else 0

        # Send RequestVote RPCs to all other nodes
        for node_name in NODES:
            if node_name != self.name:
                try:
                    response = self.send_rpc(
                        NODES[node_name]['ip'],
                        NODES[node_name]['port'],
                        'RequestVote',
                        {
                            'term': self.current_term,
                            'candidate_name': self.name,
                            'last_log_index': last_log_index,
                            'last_log_term': last_log_term
                        }
                    )

                    if response and response.get('vote_granted'):
                        votes_received += 1
                        if (votes_received > len(NODES) // 2 and 
                            self.state == 'Candidate'):  # Check if still candidate
                            self.become_leader()
                            break
                    elif response and response['term'] > self.current_term:
                        self.current_term = response['term']
                        self.state = 'Follower'
                        self.voted_for = None
                        break
                except Exception as e:
                    print(f"[{self.name}] Error requesting vote from {node_name}: {e}")

    """
    This function checks the cluster health by sending AppendEntries RPCs to all other nodes.
    It returns the number of reachable nodes.
    """
    def check_cluster_health(self):
        """Check how many nodes are reachable in the cluster"""
        reachable_nodes = 1  # Count self
        for node_name in NODES:
            if node_name != self.name:
                try:
                    response = self.send_rpc(
                        NODES[node_name]['ip'],
                        NODES[node_name]['port'],
                        'AppendEntries',  # Use as heartbeat
                        {
                            'term': self.current_term,
                            'leader_name': self.name,
                            'prev_log_index': len(self.log) - 1,
                            'prev_log_term': self.log[-1]['term'] if self.log else 0,
                            'entries': [],
                            'leader_commit': self.commit_index
                        }
                    )
                    if response is not None:
                        reachable_nodes += 1
                except Exception:
                    continue
        return reachable_nodes

    """
    This function becomes the leader for the next term. It resets the election timer and
    initializes the leader state. It then sends heartbeats to all other nodes.
    """
    def become_leader(self):
        # Check cluster health before becoming leader
        reachable_nodes = self.check_cluster_health()
        if reachable_nodes <= len(NODES) // 2:
            print(f"[{self.name}] Error becoming a leader: only {reachable_nodes}/{len(NODES)} nodes reachable")
            self.state = 'Follower'
            return

        print(f"[{self.name}] Becoming leader for term {self.current_term}")
        self.state = 'Leader'
        self.leader_id = self.name
        
        # Initialize leader state
        self.next_index = {node: len(self.log) for node in NODES if node != self.name}
        self.match_index = {node: -1 for node in NODES if node != self.name}
        
        # Send immediate heartbeat
        self.send_heartbeats()

    """
    This function sends heartbeats to all other nodes. It iterates over all nodes except the current node,
    and sends AppendEntries RPCs with the log entries starting from the next index.
    """
    def send_heartbeats(self):
        for node_name in NODES:
            if node_name != self.name:
                entries = []
                next_idx = self.next_index.get(node_name, len(self.log))
                
                if next_idx < len(self.log):
                    entries = self.log[next_idx:]
                
                self.send_append_entries(node_name, entries)

    """
    This function handles client requests to submit a value. It checks if the node is a leader,
    and if so, it appends a new entry to the log and replicates it to the followers.
    If the replication is successful, it commits the entry and applies it to the state machine.
    If the replication fails, it rolls back the log and returns an error.
    """
    def handle_client_submit(self, data):
        if self.state != 'Leader':
            return {
                'redirect': True,
                'leader_name': self.leader_id
            }

        # Append new entry to log
        entry = {
            'term': self.current_term,
            'value': data['value'],
            'index': len(self.log),
            'leader': self.name
        }
        self.log.append(entry)
        print(f"[{self.name}] New request from the client: {entry}")

        # Replicate to followers
        success_count = 1  # Count self
        for node_name in NODES:
            if node_name != self.name:
                if self.replicate_log_to_follower(node_name):
                    success_count += 1

        # If majority successful, commit and apply
        if success_count > len(NODES) // 2:
            self.commit_index = len(self.log) - 1
            self.apply_committed_entries()
            return {'success': True}
        else:
            # Roll back if replication failed, FIFO
            self.log.pop()
            return {'success': False}

    """
    This function replicates the log to a follower. It sends AppendEntries RPCs to the follower
    and updates the next index and match index accordingly.
    """
    def replicate_log_to_follower(self, follower_name):
        next_idx = self.next_index[follower_name]
        entries = self.log[next_idx:]
        
        response = self.send_append_entries(follower_name, entries)
        if response and response.get('success'):
            self.next_index[follower_name] = len(self.log)
            self.match_index[follower_name] = len(self.log) - 1
            return True
        elif response:
            self.next_index[follower_name] = max(0, self.next_index[follower_name] - 1) # If failed, decrement next_index and retry
        return False

    """
    This function sends an AppendEntries RPC to a follower. It calculates the previous log index and term,
    and sends the entries to the follower.
    """
    def send_append_entries(self, follower_name, entries):
        prev_log_index = self.next_index[follower_name] - 1
        prev_log_term = (
            self.log[prev_log_index]['term'] 
            if prev_log_index >= 0 and self.log 
            else 0
        )

        return self.send_rpc(
            NODES[follower_name]['ip'],
            NODES[follower_name]['port'],
            'AppendEntries',
            {
                'term': self.current_term,
                'leader_name': self.name,
                'prev_log_index': prev_log_index,
                'prev_log_term': prev_log_term,
                'entries': entries,
                'leader_commit': self.commit_index
            }
        )

    """
    This function handles the LeaderChange RPC. It checks if the node is a leader, and if so,
    it steps down as the leader and starts a new election.
    """
    def trigger_leader_change(self):
        if self.state == 'Leader':
            print(f"[{self.name}] Leader change requested")
            self.state = 'Follower'
            current_leader = self.leader_id
            self.voted_for = None
            self.leader_id = None
            self.reset_election_timer()
            return {'status': 'Leader ({}) stepping down'.format(current_leader)}
        return {'status': 'Not a leader'}
    
    """
    This function handles the GetStatus RPC. It returns the current state, term, leader name, 
    whether the node is a leader, and the log size. The reqest comes from the client and all of the nodes
    in the cluster are requested to return their status.
    """
    def handle_get_status(self):
        """Handle GetStatus RPC"""
        return {
            'state': self.state,
            'term': self.current_term,
            'leader_name': self.leader_id,
            'is_leader': self.state == 'Leader',
            'log_size': len(self.log)
        }


    """
    This function sends a RPC to a node. It tries to connect to the node, sends the RPC message,
    and returns the response. If the connection fails, it returns None.
    """
    def send_rpc(self, ip, port, rpc_type, data, timeout=2.0):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(timeout)
                s.connect((ip, port))
                message = json.dumps({'rpc_type': rpc_type, 'data': data})
                s.sendall(message.encode())
                response = s.recv(4096).decode()
                return json.loads(response)
            
        except socket.timeout:
            return None   
        except ConnectionRefusedError:
            return None
        except Exception as e: 
            return None


"""
This is the main function that starts the node. It checks if the correct number of arguments are provided,
and then starts the node with the given name.
"""
if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage tutorial: python node.py [node_name]")
        sys.exit(1)

    node_name = sys.argv[1]
    if node_name not in NODES:
        print(f"Invalid node name. Available nodes: {list(NODES.keys())}")
        sys.exit(1)

    node = Node(node_name)
    try:
        node.start()
    except KeyboardInterrupt:
        print(f"[{node.name}] Shutting down...")
        node.running = False
        if node.server_socket:
            node.server_socket.close()