"""
Task executor for AI summarization and PDF generation.
"""
import logging
import json
import time
from typing import Dict, Any, Optional
import aiohttp
import asyncio

logger = logging.getLogger(__name__)

class TaskExecutor:
    def __init__(self, node, api_client):
        self.node = node
        self.api_client = api_client
        self.tasks_processed = 0
    
    def execute_task(self, task_id: str, task_type: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a task based on its type."""
        logger.info(f"Executing task {task_id} of type {task_type}")
        
        try:
            if task_type == "summarization":
                result = self._execute_summarization(task_id, data)
            elif task_type == "structuring":
                result = self._execute_structuring(task_id, data)
            elif task_type == "pdf_generation":
                result = self._execute_pdf_generation(task_id, data)
            else:
                raise ValueError(f"Unknown task type: {task_type}")
            
            self.tasks_processed += 1
            return {
                "status": "success",
                "task_id": task_id,
                "task_type": task_type,
                "result": result
            }
            
        except Exception as e:
            logger.error(f"Task execution failed: {str(e)}")
            return {
                "status": "error",
                "task_id": task_id,
                "error": str(e)
            }
    
    def _execute_summarization(self, task_id: str, data: Dict[str, Any]) -> str:
        """Execute AI summarization using OpenRouter API."""
        project_description = data.get('project_description', '')
        
        if not project_description:
            raise ValueError("No project description provided")
        
        prompt = f"""Please provide a concise summary of the following project description:

{project_description}

Summary:"""
        
        response = self.api_client.call_openrouter(prompt)
        
        if not response:
            # Fallback: simple truncation
            if len(project_description) > 500:
                return project_description[:497] + "..."
            return project_description
        
        return response
    
    def _execute_structuring(self, task_id: str, data: Dict[str, Any]) -> Dict[str, str]:
        """Structure the summary into sections."""
        step_data = data.get('step_data', [])
        
        # Find the summarization result
        summary = None
        for step in step_data:
            if step['type'] == 'summarization' and step['result']:
                summary = step['result']
                break
        
        if not summary:
            raise ValueError("No summary available for structuring")
        
        prompt = f"""Based on the following project summary, extract or create the following sections:

Summary: {summary}

Please provide:
1. Abstract: A brief overview
2. Objectives: Key goals and objectives
3. Methodology: Approach and methods used
4. Outcome: Expected or achieved results

Format the response as a JSON object with keys: abstract, objectives, methodology, outcome."""
        
        response = self.api_client.call_openrouter(prompt)
        
        try:
            # Try to parse as JSON
            if response and response.strip().startswith('{'):
                import json
                structured = json.loads(response)
                
                # Ensure all required fields exist
                required_fields = ['abstract', 'objectives', 'methodology', 'outcome']
                for field in required_fields:
                    if field not in structured:
                        structured[field] = "Not specified"
                
                return structured
        except:
            pass
        
        # Fallback: create simple structure
        return {
            "abstract": summary[:200] + "..." if len(summary) > 200 else summary,
            "objectives": "Extracted from project description",
            "methodology": "To be determined based on project scope",
            "outcome": "Expected successful completion"
        }
    
    def _execute_pdf_generation(self, task_id: str, data: Dict[str, Any]) -> Dict[str, str]:
        """Generate PDF from structured content."""
        # This is handled by the leader in task_manager.py
        # Workers don't generate PDFs directly
        
        return {
            "message": "PDF generation triggered",
            "task_id": task_id,
            "status": "queued_for_pdf"
        }