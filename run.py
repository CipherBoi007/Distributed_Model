#!/usr/bin/env python3
"""
Main entry point for the distributed summarizer node.
Run with: python run.py --node-id <id>
"""
import os
from dotenv import load_dotenv
import sys
import argparse
import yaml
import uvicorn
from src.main import app, initialize_node

def load_config():
    """Load configuration from YAML file."""
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    # Recursively expand environment variable placeholders of the form ${VARNAME}
    def expand_env_in_obj(obj):
        if isinstance(obj, dict):
            return {k: expand_env_in_obj(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [expand_env_in_obj(v) for v in obj]
        if isinstance(obj, str):
            # Replace occurrences like ${VAR} with environment values (empty string if unset)
            parts = []
            i = 0
            while i < len(obj):
                start = obj.find('${', i)
                if start == -1:
                    parts.append(obj[i:])
                    break
                parts.append(obj[i:start])
                end = obj.find('}', start)
                if end == -1:
                    # malformed, keep rest
                    parts.append(obj[start:])
                    break
                env_var = obj[start+2:end]
                parts.append(os.getenv(env_var, ''))
                i = end + 1
            return ''.join(parts)
        return obj

    config = expand_env_in_obj(config)
    return config

def main():
    # Load .env file if present so placeholders in config.yaml can use it
    load_dotenv()
    parser = argparse.ArgumentParser(description='Distributed Project Summarizer Node')
    parser.add_argument('--node-id', type=int, required=True, help='Node ID (1, 2, or 3)')
    parser.add_argument('--port', type=int, help='Port to run on (overrides config)')
    parser.add_argument('--ip', type=str, help='IP address (overrides config)')
    args = parser.parse_args()
    
    # Load configuration
    config = load_config()
    
    # Find node config
    node_config = None
    for node in config['nodes']:
        if node['id'] == args.node_id:
            node_config = node
            break
    
    if not node_config:
        print(f"Error: Node ID {args.node_id} not found in config.yaml")
        sys.exit(1)
    
    # Override with command line arguments
    if args.port:
        node_config['port'] = args.port
    if args.ip:
        node_config['ip'] = args.ip
    
    # Set environment variable
    os.environ['NODE_ID'] = str(args.node_id)
    # Make current node ip/port available to the app via env
    os.environ['NODE_IP'] = str(node_config['ip'])
    os.environ['NODE_PORT'] = str(node_config['port'])
    
    # Initialize node
    print(f"Starting node {args.node_id} at {node_config['ip']}:{node_config['port']}")
    
    # Initialize application
    initialize_node(node_config, config)
    
    # Run FastAPI server
    uvicorn.run(
        app,
        host=node_config['ip'],
        port=node_config['port'],
        log_level="info"
    )

if __name__ == "__main__":
    main()