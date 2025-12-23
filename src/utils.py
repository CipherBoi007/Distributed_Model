"""
Utility functions for the distributed system.
"""
import socket
import logging
from typing import Optional, Dict, Any
import json

logger = logging.getLogger(__name__)

def get_local_ip() -> Optional[str]:
    """Get local IP address."""
    try:
        # Create a socket to get local IP
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # This doesn't actually connect, just sets up the socket
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception as e:
        logger.error(f"Failed to get local IP: {str(e)}")
        return None

def is_port_available(ip: str, port: int) -> bool:
    """Check if a port is available."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        result = sock.connect_ex((ip, port))
        sock.close()
        return result != 0  # 0 means port is in use
    except:
        return False

def validate_config(config: Dict[str, Any]) -> bool:
    """Validate configuration."""
    required_sections = ['nodes', 'network', 'api', 'tasks']
    
    for section in required_sections:
        if section not in config:
            logger.error(f"Missing config section: {section}")
            return False
    
    # Validate nodes
    if not isinstance(config['nodes'], list) or len(config['nodes']) < 1:
        logger.error("Nodes must be a list with at least one node")
        return False
    
    node_ids = []
    for node in config['nodes']:
        if 'id' not in node or 'ip' not in node or 'port' not in node:
            logger.error("Each node must have id, ip, and port")
            return False
        if node['id'] in node_ids:
            logger.error(f"Duplicate node ID: {node['id']}")
            return False
        node_ids.append(node['id'])
    
    # Validate network settings
    network = config['network']
    required_network = ['heartbeat_interval', 'leader_timeout', 'election_timeout']
    for setting in required_network:
        if setting not in network:
            logger.error(f"Missing network setting: {setting}")
            return False
    
    return True

def format_task_result(task_result: Dict[str, Any]) -> str:
    """Format task result for logging."""
    try:
        return json.dumps(task_result, indent=2)
    except:
        return str(task_result)