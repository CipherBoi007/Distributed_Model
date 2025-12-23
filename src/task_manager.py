"""
Task manager for distributed task execution.
"""
import threading
import time
import logging
import uuid
import json
import requests
from typing import Dict, List, Optional, Any
from queue import Queue, Empty
from datetime import datetime

logger = logging.getLogger(__name__)

class TaskManager:
    def __init__(self, node, task_executor):
        self.node = node
        self.task_executor = task_executor
        
        # Task queues
        self.pending_tasks: Queue = Queue()
        self.in_progress_tasks: Dict[str, Dict] = {}
        self.completed_tasks: Dict[str, Dict] = {}
        self.failed_tasks: Dict[str, Dict] = {}
        
        # Task processing thread
        self.processing_thread: Optional[threading.Thread] = None
        self.running = True
        
        # Thread safety
        self.lock = threading.RLock()
        
        # Task retry tracking
        self.task_retries: Dict[str, int] = {}
        
        logger.info("Task manager initialized")
    
    def create_task(self, project_description: str, user_email: Optional[str] = None) -> str:
        """Create a new summarization task."""
        task_id = str(uuid.uuid4())[:8]
        
        task = {
            "id": task_id,
            "project_description": project_description,
            "user_email": user_email,
            "status": "pending",
            "created_at": datetime.now().isoformat(),
            "current_step": 0,
            "steps": [
                {"type": "summarization", "status": "pending", "result": None},
                {"type": "structuring", "status": "pending", "result": None},
                {"type": "pdf_generation", "status": "pending", "result": None}
            ],
            "assigned_to": None,
            "retry_count": 0
        }
        
        with self.lock:
            self.pending_tasks.put(task)
        
        logger.info(f"Created task {task_id} for project: {project_description[:50]}...")
        return task_id
    
    def start_processing(self):
        """Start task processing thread (only for leader)."""
        if not self.node.is_leader:
            return
        
        if not self.processing_thread or not self.processing_thread.is_alive():
            self.processing_thread = threading.Thread(target=self._process_tasks, daemon=True)
            self.processing_thread.start()
            logger.info("Task processing started")
    
    def process_pending_tasks(self):
        """Process pending tasks (called from API)."""
        self._assign_tasks()
    
    def _process_tasks(self):
        """Main task processing loop for leader."""
        while self.running:
            try:
                if self.node.is_leader:
                    self._assign_tasks()
                    self._check_timeout_tasks()
                    self._cleanup_completed_tasks()
                
                time.sleep(1)  # Process every second
                
            except Exception as e:
                logger.error(f"Error in task processing: {str(e)}")
                time.sleep(2)
    
    def _assign_tasks(self):
        """Assign pending tasks to available workers."""
        with self.lock:
            # Get available workers (excluding leader if it's busy)
            available_workers = self._get_available_workers()
            
            if not available_workers:
                return
            
            # Assign tasks to workers
            assigned_count = 0
            while not self.pending_tasks.empty() and available_workers and assigned_count < len(available_workers):
                try:
                    task = self.pending_tasks.get_nowait()
                    worker = available_workers[assigned_count]
                    
                    # Find next pending step
                    next_step = None
                    for step in task['steps']:
                        if step['status'] == 'pending':
                            next_step = step
                            break
                    
                    if next_step:
                        # Update task status
                        task['status'] = 'in_progress'
                        task['assigned_to'] = worker['id']
                        next_step['status'] = 'assigned'
                        
                        # Add to in-progress tasks
                        self.in_progress_tasks[task['id']] = task
                        
                        # Assign to worker
                        self._assign_task_to_worker(task, next_step, worker)
                        
                        assigned_count += 1
                        logger.info(f"Assigned task {task['id']} step {next_step['type']} to worker {worker['id']}")
                
                except Empty:
                    break
    
    def _get_available_workers(self) -> List[Dict]:
        """Get list of available worker nodes."""
        available_workers = []
        
        # Get all alive nodes except self (leader)
        alive_nodes = self.node.get_alive_nodes()
        
        for node_id in alive_nodes:
            if node_id != self.node.node_id:
                # Check if worker is not overloaded
                worker_load = self._get_worker_load(node_id)
                if worker_load < 3:  # Max 3 tasks per worker
                    # Find node config
                    for node_config in self.node.all_nodes:
                        if node_config['id'] == node_id:
                            available_workers.append(node_config)
                            break
        
        return available_workers
    
    def _get_worker_load(self, worker_id: int) -> int:
        """Get current load of a worker."""
        load = 0
        for task in self.in_progress_tasks.values():
            if task['assigned_to'] == worker_id:
                load += 1
        return load
    
    def _assign_task_to_worker(self, task: Dict, step: Dict, worker: Dict):
        """Assign a task step to a worker."""
        assignment = {
            "task_id": task['id'],
            "task_type": step['type'],
            "data": {
                "project_description": task['project_description'],
                "step_data": task['steps'],
                "task_info": {
                    "id": task['id'],
                    "current_step": task['current_step']
                }
            }
        }
        
        # Send assignment to worker
        url = f"http://{worker['ip']}:{worker['port']}/execute_task"
        
        try:
            response = requests.post(url, json=assignment, timeout=10)
            if response.status_code == 200:
                result = response.json()
                if result['status'] == 'completed':
                    self._process_task_result(task['id'], result['result'])
                else:
                    logger.error(f"Task {task['id']} failed on worker {worker['id']}")
                    self._handle_task_failure(task['id'])
            else:
                logger.error(f"Worker {worker['id']} returned error for task {task['id']}")
                self._handle_task_failure(task['id'])
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to assign task to worker {worker['id']}: {str(e)}")
            self._handle_task_failure(task['id'])
    
    def _process_task_result(self, task_id: str, result: Dict):
        """Process result from worker."""
        with self.lock:
            if task_id not in self.in_progress_tasks:
                return
            
            task = self.in_progress_tasks[task_id]
            
            # Update step result
            for i, step in enumerate(task['steps']):
                if step['status'] == 'assigned':
                    step['status'] = 'completed'
                    step['result'] = result.get('result')
                    task['current_step'] = i + 1
                    
                    # Check if task is complete
                    if i == len(task['steps']) - 1:
                        task['status'] = 'completed'
                        task['completed_at'] = datetime.now().isoformat()
                        
                        # Move to completed tasks
                        self.completed_tasks[task_id] = task
                        del self.in_progress_tasks[task_id]
                        
                        logger.info(f"Task {task_id} completed successfully")
                        
                        # Generate final PDF
                        self._generate_final_pdf(task)
                    else:
                        # Move back to pending queue for next step
                        task['status'] = 'pending'
                        task['assigned_to'] = None
                        self.pending_tasks.put(task)
                        del self.in_progress_tasks[task_id]
                    
                    break
    
    def _generate_final_pdf(self, task: Dict):
        """Generate final PDF from completed task."""
        try:
            # Collect all results
            summary = None
            structure = None
            
            for step in task['steps']:
                if step['type'] == 'summarization' and step['result']:
                    summary = step['result']
                elif step['type'] == 'structuring' and step['result']:
                    structure = step['result']
            
            if summary and structure:
                # Generate PDF using reportlab
                from reportlab.lib.pagesizes import letter
                from reportlab.pdfgen import canvas
                from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
                from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
                
                pdf_path = f"outputs/{task['id']}_summary.pdf"
                
                # Create outputs directory if not exists
                import os
                os.makedirs("outputs", exist_ok=True)
                
                doc = SimpleDocTemplate(pdf_path, pagesize=letter)
                styles = getSampleStyleSheet()
                story = []
                
                # Title
                title_style = ParagraphStyle(
                    'CustomTitle',
                    parent=styles['Heading1'],
                    fontSize=16,
                    spaceAfter=30
                )
                story.append(Paragraph("Project Summary Report", title_style))
                
                # Summary
                story.append(Paragraph("Summary:", styles['Heading2']))
                story.append(Paragraph(summary, styles['Normal']))
                story.append(Spacer(1, 12))
                
                # Structured sections
                if isinstance(structure, dict):
                    for section, content in structure.items():
                        story.append(Paragraph(f"{section.capitalize()}:", styles['Heading3']))
                        story.append(Paragraph(content, styles['Normal']))
                        story.append(Spacer(1, 12))
                
                doc.build(story)
                logger.info(f"PDF generated: {pdf_path}")
                
        except Exception as e:
            logger.error(f"Failed to generate PDF for task {task['id']}: {str(e)}")
    
    def _handle_task_failure(self, task_id: str):
        """Handle task failure with retry logic."""
        with self.lock:
            if task_id not in self.in_progress_tasks:
                return
            
            task = self.in_progress_tasks[task_id]
            task['retry_count'] += 1
            
            max_retries = self.node.config['tasks']['max_retries']
            
            if task['retry_count'] >= max_retries:
                # Task failed permanently
                task['status'] = 'failed'
                task['failed_at'] = datetime.now().isoformat()
                self.failed_tasks[task_id] = task
                del self.in_progress_tasks[task_id]
                
                logger.error(f"Task {task_id} failed permanently after {max_retries} retries")
            else:
                # Retry task
                task['status'] = 'pending'
                task['assigned_to'] = None
                
                # Reset step status
                for step in task['steps']:
                    if step['status'] == 'assigned':
                        step['status'] = 'pending'
                        step['result'] = None
                
                self.pending_tasks.put(task)
                del self.in_progress_tasks[task_id]
                
                logger.info(f"Task {task_id} queued for retry ({task['retry_count']}/{max_retries})")
    
    def _check_timeout_tasks(self):
        """Check for timed out tasks."""
        with self.lock:
            current_time = time.time()
            timeout = self.node.config['tasks']['timeout_seconds']
            
            tasks_to_retry = []
            
            for task_id, task in list(self.in_progress_tasks.items()):
                # Check if task has been in progress too long
                if 'assigned_at' in task:
                    assigned_time = task['assigned_at']
                    if current_time - assigned_time > timeout:
                        tasks_to_retry.append(task_id)
            
            for task_id in tasks_to_retry:
                logger.warning(f"Task {task_id} timed out, retrying")
                self._handle_task_failure(task_id)
    
    def _cleanup_completed_tasks(self):
        """Clean up old completed tasks."""
        with self.lock:
            current_time = time.time()
            max_age = 3600  # 1 hour
            
            tasks_to_remove = []
            
            for task_id, task in self.completed_tasks.items():
                if 'completed_at' in task:
                    try:
                        completed_time = datetime.fromisoformat(task['completed_at']).timestamp()
                        if current_time - completed_time > max_age:
                            tasks_to_remove.append(task_id)
                    except:
                        pass
            
            for task_id in tasks_to_remove:
                del self.completed_tasks[task_id]
    
    def get_task_status(self) -> Dict:
        """Get status of all tasks."""
        with self.lock:
            return {
                "pending": self.pending_tasks.qsize(),
                "in_progress": len(self.in_progress_tasks),
                "completed": len(self.completed_tasks),
                "failed": len(self.failed_tasks),
                "node_is_leader": self.node.is_leader
            }