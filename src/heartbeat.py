"""
Heartbeat mechanism for failure detection.
"""
import threading
import time
import logging
import requests
from typing import Dict

logger = logging.getLogger(__name__)

class HeartbeatManager:
    def __init__(self, node, leader_election):
        self.node = node
        self.leader_election = leader_election
        self.running = True
        self.heartbeat_thread: Optional[threading.Thread] = None
        self.receive_thread: Optional[threading.Thread] = None
        
        # Start heartbeat threads
        self.start()
    
    def start(self):
        """Start heartbeat threads."""
        # Thread for sending heartbeats
        self.heartbeat_thread = threading.Thread(target=self._send_heartbeats, daemon=True)
        self.heartbeat_thread.start()
        
        # Thread for checking heartbeats
        self.receive_thread = threading.Thread(target=self._check_heartbeats, daemon=True)
        self.receive_thread.start()
        
        logger.info("Heartbeat manager started")
    
    def stop(self):
        """Stop heartbeat threads."""
        self.running = False
        if self.heartbeat_thread:
            self.heartbeat_thread.join(timeout=2)
        if self.receive_thread:
            self.receive_thread.join(timeout=2)
    
    def _send_heartbeats(self):
        """Send heartbeat to all other nodes."""
        while self.running:
            try:
                for other_node in self.node.all_nodes:
                    if other_node['id'] != self.node.node_id:
                        self._send_heartbeat_to_node(other_node)
                
                # Wait for interval
                time.sleep(self.node.config['network']['heartbeat_interval'])
                
            except Exception as e:
                logger.error(f"Error sending heartbeats: {str(e)}")
                time.sleep(1)
    
    def _send_heartbeat_to_node(self, target_node: Dict):
        """Send heartbeat to a specific node."""
        url = f"http://{target_node['ip']}:{target_node['port']}/heartbeat"
        data = {
            "node_id": self.node.node_id,
            "timestamp": time.time()
        }
        
        try:
            response = requests.post(url, json=data, timeout=3)
            if response.status_code == 200:
                logger.debug(f"Heartbeat sent to node {target_node['id']}")
        except requests.exceptions.RequestException as e:
            logger.debug(f"Heartbeat failed for node {target_node['id']}: {str(e)}")
    
    def receive_heartbeat(self, node_id: int):
        """Receive heartbeat from another node."""
        self.node.update_heartbeat(node_id)
    
    def _check_heartbeats(self):
        """Check for missing heartbeats."""
        while self.running:
            try:
                current_time = time.time()
                timeout = self.node.config['network']['leader_timeout']
                
                # Check leader heartbeat
                if self.node.leader_id and self.node.leader_id != self.node.node_id:
                    if node_id in self.node.alive_nodes:
                        last_seen = self.node.alive_nodes[node_id]
                        if current_time - last_seen > timeout:
                            logger.warning(f"Leader {self.node.leader_id} heartbeat timeout")
                            # Leader election will handle this
                
                time.sleep(2)  # Check every 2 seconds
                
            except Exception as e:
                logger.error(f"Error checking heartbeats: {str(e)}")
                time.sleep(1)