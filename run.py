#!/usr/bin/env python3
"""
Main entry point for the distributed summarizer node.
Run with: python run.py --node-id <id>
"""
import os
import sys
import argparse
import yaml
import uvicorn
from src.main import app, initialize_node

def load_config():
    """Load configuration from YAML file."""
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    # Replace environment variables
    for key, value in config['api'].items():
        if isinstance(value, str) and value.startswith('${') and value.endswith('}'):
            env_var = value[2:-1]
            config['api'][key] = os.getenv(env_var, '')
    
    return config

def main():
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