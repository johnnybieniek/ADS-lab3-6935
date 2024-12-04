# node.py
import socket
import threading
import time
import json
import sys
import random

# For local testing
#LOCAL_NODES = {name: {'ip': 'localhost', 'port': node['port'], 'role': node['role'], 
#               'cluster': node.get('cluster')} for name, node in NODES.items()}

# Timeout settings (in seconds)
ELECTION_TIMEOUT = (2, 4)  # Range for random election timeout to make it more realistic
HEARTBEAT_INTERVAL = 0.5  # Interval for leader to send heartbeats
MAJORITY_SIZE = 2  # Minimum votes needed for consensus in each cluster (3 nodes, need 2)


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




class Node:
    def __init__(self, name, scenario='A', failure_mode=None):
        # Basic node info
        self.name = name
        self.node_info = NODES[name]
        self.ip = self.node_info['ip']
        self.port = self.node_info['port']
        self.role = self.node_info['role']
        self.cluster = self.node_info.get('cluster')

        # Raft state
        self.state = 'Follower'  # Start as follower
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
        
        # Replica-specific attributes
        self.is_primary = self.role.startswith('primary')
        self.is_replica = self.role.startswith('replica')
        self.replica_group = []
        self.last_sync_time = time.time()
        
        # Set up replica groups based on cluster
        if self.cluster == 'a':
            self.replica_group = CLUSTER_A
        elif self.cluster == 'b':
            self.replica_group = CLUSTER_B
            
        # Initialize replica states for monitoring
        self.replica_states = {
            node: {'last_updated': time.time(), 'active': True}
            for node in self.replica_group if node != self.name
        }

        # Failure simulation
        self.failure_mode = failure_mode
        self.scenario = scenario
        self.has_failed = False
        
        # Log setup
        self.log_filename = f"CISC6935-{self.name}"
        self.is_coordinator = (self.role == 'coordinator')
        
        # Account setup
        self.account_file = None
        self.account_name = None
        if self.cluster == 'a':
            self.account_file = f'account_A_{self.name}.txt'
            self.account_name = 'A'
        elif self.cluster == 'b':
            self.account_file = f'account_B_{self.name}.txt'
            self.account_name = 'B'

        # Initialize account if needed
        if self.account_file:
            self.initialize_account(scenario)

        # Transaction state
        self.transaction_state = {
            'status': None,
            'transaction_id': None,
            'participants_ready': set(),
            'participants_committed': set(),
            'transaction_log': [],
            'current_transaction': None
        }

    def get_primary_node(self):
        """Helper function to get primary node name for current cluster"""
        return next(name for name, info in NODES.items() 
                if info['role'] == f'primary-{self.cluster}')

    def sync_with_replicas(self, new_balance):
        """Simplified and more robust sync process"""
        if not self.is_primary:
            return True
                
        print(f"[{self.name}] Primary starting sync for balance: {new_balance}")
        successful_syncs = 1  # Count self
        
        for replica in self.replica_group:
            if replica == self.name:
                continue
            
            try:
                # Single attempt to sync with shorter timeout
                response = self.send_rpc(
                    NODES[replica]['ip'],
                    NODES[replica]['port'],
                    'SyncBalance',
                    {'balance': new_balance},
                    timeout=1.0  # Shorter timeout
                )
                
                if response and response.get('success'):
                    successful_syncs += 1
                    print(f"[{self.name}] Successfully synced with {replica}")
                else:
                    print(f"[{self.name}] Failed to sync with {replica}")
                    
            except Exception as e:
                print(f"[{self.name}] Error syncing with {replica}: {str(e)}")
        
        # More lenient majority requirement - need only one replica to sync
        sync_success = successful_syncs > 1
        print(f"[{self.name}] Sync complete. Success: {sync_success} ({successful_syncs} nodes)")
        return sync_success

    def handle_sync_balance(self, data):
        """Simplified sync handler - just accept and apply the new balance"""
        try:
            if not self.is_replica:
                return {'success': False, 'error': 'Not a replica'}
            
            new_balance = data.get('balance')
            if new_balance is None:
                return {'success': False, 'error': 'No balance provided'}
            
            print(f"[{self.name}] Updating balance to {new_balance} from primary")
            with open(self.account_file, 'w') as f:
                f.write(str(new_balance))
            
            return {'success': True}
        except Exception as e:
            print(f"[{self.name}] Error in sync balance: {str(e)}")
            return {'success': False, 'error': str(e)}

    def initialize_account(self, scenario='A'):
        """Initialize account file with balance based on scenario"""
        scenarios = {
            'A': {'A': 200.0, 'B': 300.0},
            'B': {'A': 90.0, 'B': 50.0},
            'C': {'A': 200.0, 'B': 300.0}
        }
       
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
        """Modified to handle replica reads"""
        if not self.account_file:
            return None
            
        try:
            with open(self.account_file, 'r') as f:
                balance = float(f.read().strip())
                
            # If this is a replica and the last sync was too long ago,
            # try to get fresh data from primary
            if self.is_replica and time.time() - self.last_sync_time > 5:  # 5 second threshold
                self.recover_replica()
                
            return balance
        except Exception as e:
            print(f"[{self.name}] Error reading balance: {str(e)}")
            return None

    def recover_replica(self):
        """Recover a replica that was down"""
        if not self.is_replica:
            return
            
        try:
            # Find primary for this cluster
            primary_node = next(node for node, info in NODES.items() 
                           if info['role'] == f'primary-{self.cluster}')
            
            print(f"[{self.name}] Attempting to recover state from primary {primary_node}")
            
            # Get current balance from primary
            response = self.send_rpc(
                NODES[primary_node]['ip'],
                NODES[primary_node]['port'],
                'GetBalance',
                {}
            )
            
            if response and 'balance' in response:
                self.update_balance(response['balance'])
                self.last_sync_time = time.time()
                print(f"[{self.name}] Successfully recovered state from primary")
            else:
                print(f"[{self.name}] Failed to recover state from primary")
                
        except Exception as e:
            print(f"[{self.name}] Error in replica recovery: {str(e)}")
        
    def simulate_failure(self):
        """Simulate node failure with sleep"""
        print(f"[{self.name}] Simulating node failure...")
        time.sleep(10)  # 10 second delay to simulate crash
        self.has_failed = True

    def update_balance(self, new_balance):
        """Modified to include replica synchronization"""
        if not self.account_file:
            return False
            
        try:
            # If this is a replica, just update local balance
            if self.is_replica:
                with open(self.account_file, 'w') as f:
                    f.write(str(new_balance))
                return True
                
            # If this is primary, sync with replicas first
            sync_success = self.sync_with_replicas(new_balance)
            if sync_success:
                with open(self.account_file, 'w') as f:
                    f.write(str(new_balance))
                return True
            return False
            
        except Exception as e:
            print(f"[{self.name}] Error updating balance: {str(e)}")
            return False

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
                NODES['node2-a1']['ip'],
                NODES['node2-a1']['port'],
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
                    NODES['node3-b1']['ip'],
                    NODES['node3-b1']['port'],
                    'Prepare',
                    prepare_data
                )
                
                if node_b_response and node_b_response.get('success'):
                    self.transaction_state['participants_ready'].add('node2-a1')
                    self.transaction_state['participants_ready'].add('node3-b1')
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
    
    def handle_transaction_request(self, data):
        """Modified to work with replica system"""
        if self.has_failed:
            return {'success': False, 'error': 'Node in failed state'}

        if not self.is_coordinator:
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

        # Get primary nodes for transaction
        node_a_primary = next(node for node, info in NODES.items() 
                            if info['role'] == 'primary-a')
        node_b_primary = next(node for node, info in NODES.items() 
                            if info['role'] == 'primary-b')

        self.transaction_state.update({
            'transaction_id': transaction_id,
            'status': 'preparing',
            'current_transaction': data,
            'participants_ready': set(),
            'start_time': time.time()
        })

        print(f"[{self.name}] COORDINATOR: Sending PREPARE to primaries")
        prepare_success = self.send_prepare_to_participants(transaction_id, data)
        
        if prepare_success:
            print(f"[{self.name}] COORDINATOR: All participants ready, initiating COMMIT")
            commit_success = self.send_commit_to_participants(transaction_id)
            
            if commit_success:
                print(f"[{self.name}] COORDINATOR: Transaction committed successfully")
                self.transaction_state['status'] = 'committed'
                response = {'success': True, 'status': 'committed'}
                if self.failure_mode == "coordinator_after_commit" and not self.has_failed:
                    failure_thread = threading.Thread(target=self.delayed_failure_simulation)
                    failure_thread.daemon = True
                    failure_thread.start()
                return response
            else:
                print(f"[{self.name}] COORDINATOR: Commit failed")
                self.transaction_state['status'] = 'failed'
                return {'success': False, 'error': 'Commit failed'}
        else:
            print(f"[{self.name}] COORDINATOR: Prepare failed, initiating ABORT")
            self.send_abort_to_participants(transaction_id)
            self.transaction_state['status'] = 'aborted'
            return {'success': False, 'status': 'aborted', 'error': 'Prepare failed'}

    def handle_prepare(self, data):
        """
        Handle prepare request from coordinator for 2PC protocol.
        Includes failure simulation for testing scenarios.
        """
        try:
            # First, check if we should simulate failure BEFORE prepare
            if self.name == 'node2-a1' and self.failure_mode:
                if self.failure_mode == 'before_prepare' and not self.has_failed:
                    print(f"[{self.name}] Simulating node2 failure before prepare...")
                    self.has_failed = True
                    self.simulate_failure()
                    return None
                
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

                # Prepare response before simulating failure
                response = {
                    'success': True,
                    'status': 'ready',
                    'node': self.name,
                    'transaction_id': transaction_id,
                    'bonus_amount': bonus_amount if transaction_type == 'bonus' and self.account_name == 'A' else None
                }

                # Check if we should simulate failure AFTER prepare
                if self.name == 'node2-a1' and self.failure_mode == 'after_prepare' and not self.has_failed:
                    print(f"[{self.name}] Node2 sending READY response before simulated failure")
                    # Create a thread to simulate failure after response is sent
                    failure_thread = threading.Thread(target=self.delayed_failure_simulation)
                    failure_thread.daemon = True
                    failure_thread.start()

                return response
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

    def delayed_failure_simulation(self):
        """Simulate failure after a short delay to allow response to be sent"""
        time.sleep(0.1)  # Short delay to ensure response is sent
        print(f"[{self.name}] Node2 simulating failure after prepare...")
        self.has_failed = True
        self.simulate_failure()

    def handle_commit(self, data):
        """Simplified commit handler"""
        try:
            transaction_id = data.get('transaction_id')
            print(f"\n[{self.name}] Received COMMIT for transaction {transaction_id}")
            
            if self.is_primary:
                # Primary calculates and updates
                transaction = self.transaction_state.get('current_transaction')
                current_balance = self.get_balance()
                
                # Calculate new balance
                if transaction['type'] == 'transfer':
                    if self.account_name == 'A':
                        new_balance = current_balance - 100
                    else:
                        new_balance = current_balance + 100
                else:  # bonus
                    if self.account_name == 'A':
                        new_balance = current_balance + (current_balance * 0.2)
                    else:
                        bonus_amount = transaction.get('bonus_amount', 0)
                        new_balance = current_balance + bonus_amount
                
                # Update own balance first
                with open(self.account_file, 'w') as f:
                    f.write(str(new_balance))
                print(f"[{self.name}] Updated own balance to {new_balance}")
                
                # Then sync with replicas
                self.sync_with_replicas(new_balance)
                
            else:
                # Replicas just update to what they receive
                new_balance = float(data.get('new_balance', 0))
                with open(self.account_file, 'w') as f:
                    f.write(str(new_balance))
                print(f"[{self.name}] Updated replica balance to {new_balance}")
            
            self.transaction_state['status'] = 'committed'
            return {'success': True, 'new_balance': new_balance}
                
        except Exception as e:
            print(f"[{self.name}] Error in commit: {str(e)}")
            return {'success': False, 'error': str(e)}

    def get_primary_node(self):
        """Helper function to get primary node name for current cluster"""
        try:
            return next(name for name, info in NODES.items() 
                    if info['role'] == f'primary-{self.cluster}')
        except StopIteration:
            print(f"[{self.name}] Error: Could not find primary for cluster {self.cluster}")
            return None

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
                    NODES['node2-a1']['ip'],
                    NODES['node2-a1']['port'],
                    'SetupScenario',
                    data
                )
                
                # Send to node3 (Account B)
                response_B = self.send_rpc(
                    NODES['node3-b1']['ip'],
                    NODES['node3-b1']['port'],
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
        self.server_socket.bind(('', self.port))
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
        """Modified to handle replica-related RPCs"""
        try:
            data = client_socket.recv(4096).decode()
            if data:
                request = json.loads(data)
                rpc_type = request['rpc_type']
                response = {}

                with self.lock:
                    # Handle replica-specific RPCs
                    if rpc_type == 'SyncBalance':
                        response = self.handle_sync_balance(request['data'])
                    elif rpc_type == 'GetBalance':
                        response = {'balance': self.get_balance()}
                    # Handle existing RPCs
                    elif rpc_type == 'RequestVote':
                        response = self.handle_request_vote(request['data'])
                    elif rpc_type == 'AppendEntries':
                        response = self.handle_append_entries(request['data'])
                    elif rpc_type == 'SetupScenario':
                        response = self.handle_setup_scenario(request['data'])
                    elif rpc_type == 'Prepare':
                        response = self.handle_prepare(request['data'])
                    elif rpc_type == 'Commit':
                        response = self.handle_commit(request['data'])
                    elif rpc_type == 'Abort':
                        response = self.handle_abort(request['data'])
                    elif rpc_type == 'TransactionRequest':
                        response = self.handle_transaction_request(request['data'])
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
        """Handle GetStatus RPC with forced sync for replicas"""
        # If this is a replica, sync with primary first
        if self.is_replica:
            try:
                primary_node = next(name for name, info in NODES.items() 
                            if info['role'] == f'primary-{self.cluster}')
                
                print(f"[{self.name}] Forcing sync with primary {primary_node} before status")
                response = self.send_rpc(
                    NODES[primary_node]['ip'],
                    NODES[primary_node]['port'],
                    'GetBalance',
                    {}
                )
                
                if response and 'balance' in response:
                    primary_balance = response['balance']
                    with open(self.account_file, 'w') as f:
                        f.write(str(primary_balance))
                    print(f"[{self.name}] Updated balance to {primary_balance} from primary")
                else:
                    print(f"[{self.name}] Could not sync with primary before status")
            except Exception as e:
                print(f"[{self.name}] Error syncing with primary: {str(e)}")

        # Get current balance if this is a node with an account
        balance = None
        if self.account_file:
            try:
                with open(self.account_file, 'r') as f:
                    balance = float(f.read().strip())
            except Exception as e:
                print(f"[{self.name}] Error reading balance: {str(e)}")

        return {
            'state': self.state,
            'term': self.current_term,
            'leader_name': self.leader_id,
            'is_leader': self.state == 'Leader',
            'log_size': len(self.log),
            'role': self.role,
            'cluster': self.cluster if self.cluster else 'coordinator',
            'balance': balance
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