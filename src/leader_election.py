"""
Leader election using Bully Algorithm.
"""
import threading
import time
import logging
import uuid
import requests
from typing import Optional
from .api_client import APIClient

logger = logging.getLogger(__name__)

class LeaderElection:
    def __init__(self, node, task_manager):
        self.node = node
        self.task_manager = task_manager
        self.election_in_progress = False
        self.election_id: Optional[str] = None
        self.election_thread: Optional[threading.Thread] = None
        self.running = True
        
        # Start election on initialization
        self.start_election()
    
    def start(self):
        """Start election thread."""
        self.election_thread = threading.Thread(target=self._monitor_leader, daemon=True)
        self.election_thread.start()
        logger.info("Leader election monitor started")
    
    def stop(self):
        """Stop election thread."""
        self.running = False
        if self.election_thread:
            self.election_thread.join(timeout=2)
    
    def _monitor_leader(self):
        """Monitor leader status and trigger election if needed."""
        while self.running:
            try:
                if self.node.leader_id and not self.election_in_progress:
                    # Check if leader is alive
                    if not self.node.is_node_alive(self.node.leader_id):
                        logger.warning(f"Leader {self.node.leader_id} appears dead, starting election")
                        self.start_election()
                
                time.sleep(2)  # Check every 2 seconds
            except Exception as e:
                logger.error(f"Error in leader monitor: {str(e)}")
                time.sleep(1)
    
    def start_election(self):
        """Start a new election using Bully Algorithm."""
        if self.election_in_progress:
            return
        
        self.election_in_progress = True
        self.election_id = str(uuid.uuid4())
        
        election_thread = threading.Thread(target=self._run_election, daemon=True)
        election_thread.start()
    
    def _run_election(self):
        """Execute Bully Algorithm."""
        try:
            higher_nodes = self.node.get_higher_nodes()
            
            if not higher_nodes:
                # No higher nodes, declare self as leader
                self.declare_leader()
                return
            
            # Send election messages to all higher nodes
            responses = []
            for higher_node in higher_nodes:
                try:
                    response = self._send_election_message(higher_node)
                    if response:
                        responses.append(True)
                except Exception as e:
                    logger.warning(f"Failed to contact node {higher_node['id']}: {str(e)}")
            
            # If no responses from higher nodes, declare self as leader
            if not any(responses):
                time.sleep(self.node.config['network']['election_timeout'])
                self.declare_leader()
            
        except Exception as e:
            logger.error(f"Election failed: {str(e)}")
        finally:
            self.election_in_progress = False
    
    def _send_election_message(self, target_node: Dict) -> bool:
        """Send election message to another node."""
        url = f"http://{target_node['ip']}:{target_node['port']}/election"
        data = {
            "node_id": self.node.node_id,
            "election_id": self.election_id
        }
        
        try:
            response = requests.post(url, json=data, timeout=5)
            return response.status_code == 200
        except requests.exceptions.RequestException as e:
            logger.debug(f"Node {target_node['id']} not reachable: {str(e)}")
            return False
    
    def receive_election_message(self, sender_id: int, election_id: str):
        """Receive election message from another node."""
        if not self.election_id or election_id != self.election_id:
            logger.info(f"Received election message from node {sender_id}")
            
            # Respond to sender and start own election
            self._send_election_response(sender_id)
            self.start_election()
    
    def _send_election_response(self, sender_id: int):
        """Send OK response to election message."""
        sender_node = None
        for node in self.node.all_nodes:
            if node['id'] == sender_id:
                sender_node = node
                break
        
        if sender_node:
            url = f"http://{sender_node['ip']}:{sender_node['port']}/ok"
            try:
                requests.post(url, json={"node_id": self.node.node_id}, timeout=5)
            except:
                pass  # Silently fail
    
    def receive_leader_announcement(self, leader_id: int):
        """Receive leader announcement."""
        logger.info(f"Received leader announcement: node {leader_id} is leader")
        self.node.set_leader(leader_id)
        
        # If this node was leader but no longer is, stop task processing
        if self.node.is_leader:
            logger.info("This node is the new leader, starting task processing")
        else:
            logger.info("This node is now a worker")
    
    def declare_leader(self):
        """Declare self as leader and announce to all nodes."""
        logger.info(f"Node {self.node.node_id} declaring itself as leader")
        
        # Set self as leader
        self.node.set_leader(self.node.node_id)
        
        # Announce to all other nodes
        for other_node in self.node.all_nodes:
            if other_node['id'] != self.node.node_id:
                self._announce_leader_to_node(other_node)
        
        # Start task processing
        self.task_manager.start_processing()
    
    def _announce_leader_to_node(self, target_node: Dict):
        """Announce leadership to another node."""
        url = f"http://{target_node['ip']}:{target_node['port']}/leader"
        data = {"leader_id": self.node.node_id}
        
        try:
            requests.post(url, json=data, timeout=5)
        except requests.exceptions.RequestException as e:
            logger.warning(f"Failed to announce leadership to node {target_node['id']}: {str(e)}")