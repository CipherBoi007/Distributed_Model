"""
Node class representing a single node in the distributed system.
"""
import threading
import time
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime

logger = logging.getLogger(__name__)

class Node:
    def __init__(self, node_id: int, ip: str, port: int, config: Dict):
        self.node_id = node_id
        self.ip = ip
        self.port = port
        self.config = config
        
        # Node state
        self.is_leader = False
        self.leader_id: Optional[int] = None
        self.last_heartbeat = time.time()
        
        # Node registry
        self.all_nodes: List[Dict] = config['nodes']
        self.alive_nodes: Dict[int, float] = {}  # node_id -> last_seen
        
        # Thread safety
        self.lock = threading.RLock()
        
        # Initialize alive nodes (include self)
        with self.lock:
            self.alive_nodes[self.node_id] = time.time()
        
        logger.info(f"Node {node_id} created at {ip}:{port}")

    def get_address(self) -> str:
        """Get node address as string."""
        return f"{self.ip}:{self.port}"

    def get_leader_node(self) -> Optional[Dict]:
        """Get leader node configuration."""
        if not self.leader_id:
            return None
        
        for node in self.all_nodes:
            if node['id'] == self.leader_id:
                return node
        return None

    def update_heartbeat(self, node_id: int):
        """Update heartbeat timestamp for a node."""
        with self.lock:
            self.alive_nodes[node_id] = time.time()
            if node_id == self.leader_id:
                self.last_heartbeat = time.time()
            logger.debug(f"Heartbeat received from node {node_id}")

    def get_alive_nodes(self) -> List[int]:
        """Get list of alive node IDs."""
        with self.lock:
            current_time = time.time()
            timeout = self.config['network']['leader_timeout']
            
            # Remove dead nodes
            dead_nodes = []
            for node_id, last_seen in self.alive_nodes.items():
                if current_time - last_seen > timeout:
                    dead_nodes.append(node_id)
            
            for node_id in dead_nodes:
                del self.alive_nodes[node_id]
                logger.warning(f"Node {node_id} marked as dead")
            
            return list(self.alive_nodes.keys())

    def get_higher_nodes(self) -> List[Dict]:
        """Get nodes with higher ID than current node."""
        higher_nodes = []
        for node in self.all_nodes:
            if node['id'] > self.node_id:
                higher_nodes.append(node)
        return higher_nodes

    def get_lower_nodes(self) -> List[Dict]:
        """Get nodes with lower ID than current node."""
        lower_nodes = []
        for node in self.all_nodes:
            if node['id'] < self.node_id:
                lower_nodes.append(node)
        return lower_nodes

    def set_leader(self, leader_id: int):
        """Set the leader node."""
        with self.lock:
            if leader_id == self.node_id:
                self.is_leader = True
                logger.info(f"Node {self.node_id} is now the LEADER")
            else:
                self.is_leader = False
                logger.info(f"Node {self.node_id} acknowledges node {leader_id} as leader")
            
            self.leader_id = leader_id
            self.last_heartbeat = time.time()

    def is_node_alive(self, node_id: int) -> bool:
        """Check if a node is alive."""
        with self.lock:
            if node_id not in self.alive_nodes:
                return False
            
            current_time = time.time()
            timeout = self.config['network']['leader_timeout']
            return current_time - self.alive_nodes[node_id] <= timeout

    def to_dict(self) -> Dict:
        """Convert node to dictionary."""
        return {
            "id": self.node_id,
            "ip": self.ip,
            "port": self.port,
            "is_leader": self.is_leader,
            "leader_id": self.leader_id,
            "alive_nodes": self.get_alive_nodes()
        }