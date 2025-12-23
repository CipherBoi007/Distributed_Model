"""
Main FastAPI application and node initialization.
"""
from fastapi import FastAPI, HTTPException, BackgroundTasks, Request
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
import threading
import time
import logging
from typing import Dict, List, Optional, Any
import os

from .node import Node
from .leader_election import LeaderElection
from .heartbeat import HeartbeatManager
from .task_manager import TaskManager
from .task_executor import TaskExecutor
from .api_client import APIClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/node_{os.getenv("NODE_ID", "unknown")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Global instances
app = FastAPI(title="Distributed Project Summarizer")
node: Optional[Node] = None
leader_election: Optional[LeaderElection] = None
heartbeat_manager: Optional[HeartbeatManager] = None
task_manager: Optional[TaskManager] = None
task_executor: Optional[TaskExecutor] = None

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static files and templates
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# Pydantic models
class TaskSubmission(BaseModel):
    project_description: str
    user_email: Optional[str] = None

class TaskAssignment(BaseModel):
    task_id: str
    task_type: str
    data: Dict[str, Any]

class HeartbeatPing(BaseModel):
    node_id: int
    timestamp: float

class ElectionMessage(BaseModel):
    node_id: int
    election_id: str

# Initialize node
def initialize_node(node_config: Dict, global_config: Dict):
    """Initialize the node with configuration."""
    global node, leader_election, heartbeat_manager, task_manager, task_executor
    
    node_id = int(os.getenv('NODE_ID', node_config['id']))
    
    # Create node instance
    node = Node(
        node_id=node_id,
        ip=node_config['ip'],
        port=node_config['port'],
        config=global_config
    )
    
    # Create API client
    api_client = APIClient(global_config['api'])
    
    # Create task executor
    task_executor = TaskExecutor(node, api_client)
    
    # Create task manager
    task_manager = TaskManager(node, task_executor)
    
    # Create leader election
    leader_election = LeaderElection(node, task_manager)
    
    # Create heartbeat manager
    heartbeat_manager = HeartbeatManager(node, leader_election)
    
    # Start background threads
    heartbeat_manager.start()
    leader_election.start()
    
    logger.info(f"Node {node_id} initialized at {node.ip}:{node.port}")

# API Endpoints
@app.get("/")
async def root(request: Request):
    """Home page with system status."""
    if not node:
        raise HTTPException(status_code=500, detail="Node not initialized")
    
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "node_id": node.node_id,
            "is_leader": node.is_leader,
            "leader_id": node.leader_id,
            "alive_nodes": node.get_alive_nodes(),
            "task_queue": task_manager.get_task_status() if task_manager else {},
            "node_address": f"{node.ip}:{node.port}"
        }
    )

@app.post("/heartbeat")
async def receive_heartbeat(heartbeat: HeartbeatPing):
    """Receive heartbeat from other nodes."""
    if not heartbeat_manager:
        raise HTTPException(status_code=500, detail="Heartbeat manager not initialized")
    
    heartbeat_manager.receive_heartbeat(heartbeat.node_id)
    return {"status": "acknowledged"}

@app.post("/election")
async def receive_election_message(message: ElectionMessage):
    """Receive election message for Bully Algorithm."""
    if not leader_election:
        raise HTTPException(status_code=500, detail="Leader election not initialized")
    
    leader_election.receive_election_message(message.node_id, message.election_id)
    return {"status": "received"}

@app.post("/leader")
async def announce_leader(announcement: Dict):
    """Receive leader announcement."""
    if not leader_election:
        raise HTTPException(status_code=500, detail="Leader election not initialized")
    
    leader_id = announcement.get("leader_id")
    if leader_id is not None:
        leader_election.receive_leader_announcement(leader_id)
    return {"status": "acknowledged"}

@app.post("/submit_task")
async def submit_task(task: TaskSubmission, background_tasks: BackgroundTasks):
    """Submit a new project summarization task."""
    if not task_manager:
        raise HTTPException(status_code=500, detail="Task manager not initialized")
    
    if not node.is_leader:
        # Forward to leader if this is not the leader
        leader = node.get_leader_node()
        if not leader:
            raise HTTPException(status_code=503, detail="No leader available")
        
        # Forward request
        import aiohttp
        import json
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"http://{leader['ip']}:{leader['port']}/submit_task",
                    json=task.dict(),
                    timeout=10
                ) as response:
                    return await response.json()
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"Failed to forward to leader: {str(e)}")
    
    # Process task as leader
    task_id = task_manager.create_task(task.project_description, task.user_email)
    background_tasks.add_task(task_manager.process_pending_tasks)
    
    return {
        "task_id": task_id,
        "status": "submitted",
        "message": f"Task submitted to leader node {node.node_id}"
    }

@app.post("/execute_task")
async def execute_task(assignment: TaskAssignment):
    """Execute a task assigned by leader."""
    if not task_executor:
        raise HTTPException(status_code=500, detail="Task executor not initialized")
    
    logger.info(f"Node {node.node_id} executing task {assignment.task_id} of type {assignment.task_type}")
    
    try:
        result = task_executor.execute_task(
            assignment.task_id,
            assignment.task_type,
            assignment.data
        )
        
        return {
            "task_id": assignment.task_id,
            "status": "completed",
            "result": result
        }
    except Exception as e:
        logger.error(f"Task execution failed: {str(e)}")
        return {
            "task_id": assignment.task_id,
            "status": "failed",
            "error": str(e)
        }

@app.get("/status")
async def get_status():
    """Get node status."""
    if not node:
        raise HTTPException(status_code=500, detail="Node not initialized")
    
    return {
        "node_id": node.node_id,
        "is_leader": node.is_leader,
        "leader_id": node.leader_id,
        "ip": node.ip,
        "port": node.port,
        "alive_nodes": node.get_alive_nodes(),
        "last_heartbeat": node.last_heartbeat,
        "tasks_processed": task_executor.tasks_processed if task_executor else 0
    }

@app.get("/download/{task_id}")
async def download_pdf(task_id: str):
    """Download generated PDF."""
    pdf_path = f"outputs/{task_id}_summary.pdf"
    
    if not os.path.exists(pdf_path):
        raise HTTPException(status_code=404, detail="PDF not found")
    
    return FileResponse(
        pdf_path,
        media_type='application/pdf',
        filename=f"project_summary_{task_id}.pdf"
    )

@app.on_event("shutdown")
async def shutdown_event():
    """Clean shutdown."""
    if heartbeat_manager:
        heartbeat_manager.stop()
    if leader_election:
        leader_election.stop()
    logger.info("Node shutdown complete")