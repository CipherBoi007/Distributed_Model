"""
API client for external services (OpenRouter).
"""
import logging
import requests
import json
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

class APIClient:
    def __init__(self, api_config: Dict[str, Any]):
        self.api_key = api_config.get('openrouter_api_key', '')
        self.base_url = api_config.get('openrouter_base_url', 'https://openrouter.ai/api/v1')
        self.model = api_config.get('model', 'mistralai/mixtral-8x7b-instruct')
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
    
    def call_openrouter(self, prompt: str, max_tokens: int = 500) -> Optional[str]:
        """Call OpenRouter API for AI completion."""
        if not self.api_key:
            logger.warning("OpenRouter API key not configured")
            return None
        
        try:
            payload = {
                "model": self.model,
                "messages": [
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                "max_tokens": max_tokens,
                "temperature": 0.7
            }
            
            response = requests.post(
                f"{self.base_url}/chat/completions",
                headers=self.headers,
                json=payload,
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                content = result.get('choices', [{}])[0].get('message', {}).get('content', '')
                return content.strip()
            else:
                logger.error(f"OpenRouter API error: {response.status_code} - {response.text}")
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"OpenRouter API request failed: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error in OpenRouter call: {str(e)}")
            return None