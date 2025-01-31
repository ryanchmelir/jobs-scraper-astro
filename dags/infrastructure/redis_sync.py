"""
Redis sync module for maintaining job and company caches.
Handles both incremental and full syncs from PostgreSQL to Redis.
"""
from typing import Dict, Optional, List, Type
import redis
import re
import logging
import traceback
from datetime import datetime
import time
import json

logger = logging.getLogger(__name__)

class RedisCache:
    """Redis cache manager for job listings data."""
    
    def __init__(
        self, 
        host: str, 
        port: int,
        socket_timeout: int = 30,
        socket_connect_timeout: int = 30,
        retry_on_timeout: bool = True,
        decode_responses: bool = True,
        **kwargs
    ):
        """Initialize Redis connection with minimal configuration."""
        logger.info(f"Initializing Redis connection to {host}:{port}")
        
        try:
            # Create a single Redis instance with connection pooling
            self.redis = redis.Redis(
                host=host,
                port=port,
                socket_timeout=socket_timeout,
                socket_connect_timeout=socket_connect_timeout,
                decode_responses=decode_responses,
                retry_on_timeout=retry_on_timeout
            )
            
            # Test the connection with retry
            max_retries = 3
            retry_count = 0
            last_error = None
            
            while retry_count < max_retries:
                try:
                    self.redis.ping()
                    logger.info("Successfully connected to Redis")
                    break
                except Exception as e:
                    last_error = e
                    retry_count += 1
                    logger.warning(f"Redis connection attempt {retry_count} failed: {str(e)}")
                    if retry_count < max_retries:
                        time.sleep(1)
            
            if retry_count == max_retries:
                raise last_error
                
        except Exception as e:
            logger.error("Failed to initialize Redis client")
            logger.error(f"Error type: {type(e).__name__}")
            logger.error(f"Error message: {str(e)}")
            raise
        
        # TTL values in seconds
        self.TTL = {
            'jobs:recent': 3600,        # 1 hour
            'job': 21600,               # 6 hours
            'company': 86400,           # 24 hours
            'company:jobs': 7200,       # 2 hours
            'dept': 7200,               # 2 hours
            'location': 7200,           # 2 hours
            'search': 7200,             # 2 hours
        }
    
    def _test_connection(self) -> None:
        """Test Redis connection."""
        try:
            self.redis.ping()
            logger.info("Successfully connected to Redis")
        except Exception as e:
            logger.error("Failed to connect to Redis")
            logger.error(f"Error: {type(e).__name__}: {str(e)}")
            raise
    
    def _normalize_text(self, text: str) -> str:
        """Normalize text for search indexing."""
        if not text:
            return ""
        text = text.lower()
        text = re.sub(r'[^\w\s]', ' ', text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text
    
    def _normalize_location(self, location: str) -> str:
        """Normalize location for indexing."""
        if not location:
            return ""
        return self._normalize_text(location)

    def sync_companies_batch(self, companies: List[Dict]) -> None:
        """Sync multiple companies to Redis efficiently."""
        logger.info(f"Starting sync of {len(companies)} companies")
        
        pipe = self.redis.pipeline()
        
        for company in companies:
            company_key = f"company:{company['id']}"
            company_data = {
                'id': str(company['id']),
                'name': company['name'],
                'active': '1' if company['active'] else '0'
            }
            pipe.hset(company_key, mapping=company_data)
            pipe.expire(company_key, self.TTL['company'])
        
        pipe.execute()
        logger.info(f"Successfully synced {len(companies)} companies")

    def sync_company(self, company_id: int, name: str, active: bool) -> None:
        """Sync a single company to Redis."""
        company_key = f"company:{company_id}"
        company_data = {
            'id': str(company_id),
            'name': name,
            'active': '1' if active else '0'
        }
        
        pipe = self.redis.pipeline()
        pipe.hset(company_key, mapping=company_data)
        pipe.expire(company_key, self.TTL['company'])
        pipe.execute()

    def sync_job(self, job_data: Dict) -> None:
        """Sync a job to Redis with all its indices."""
        job_id = str(job_data['id'])
        
        # Prepare job hash data
        job_hash = {
            'id': job_id,
            'title': job_data['title'],
            'company_id': str(job_data['company_id']),
            'location': job_data.get('location', ''),
            'department': job_data.get('department', ''),
            'url': job_data.get('url', ''),
            'first_seen': str(job_data['first_seen'].timestamp()),
            'active': '1' if job_data['active'] else '0'
        }
        
        pipe = self.redis.pipeline()
        
        # Store job details
        job_key = f"job:{job_id}"
        pipe.hset(job_key, mapping=job_hash)
        pipe.expire(job_key, self.TTL['job'])
        
        if job_data['active']:
            score = job_data['first_seen'].timestamp()
            
            # Add to recent jobs
            pipe.zadd('jobs:recent', {job_id: score})
            pipe.expire('jobs:recent', self.TTL['jobs:recent'])
            
            # Add to company jobs
            company_jobs_key = f"company:jobs:{job_data['company_id']}"
            pipe.zadd(company_jobs_key, {job_id: score})
            pipe.expire(company_jobs_key, self.TTL['company:jobs'])
            
            # Add to department index
            if job_data.get('department'):
                dept_key = f"dept:{self._normalize_text(job_data['department'])}"
                pipe.zadd(dept_key, {job_id: score})
                pipe.expire(dept_key, self.TTL['dept'])
            
            # Add to location index
            if job_data.get('location'):
                loc_key = f"location:{self._normalize_location(job_data['location'])}"
                pipe.zadd(loc_key, {job_id: score})
                pipe.expire(loc_key, self.TTL['location'])
            
            # Add to search index (title words)
            title_words = set(self._normalize_text(job_data['title']).split())
            for word in title_words:
                if len(word) > 2:  # Skip very short words
                    search_key = f"search:title:{word}"
                    pipe.zadd(search_key, {job_id: score})
                    pipe.expire(search_key, self.TTL['search'])
        else:
            # Remove from all indices if job is inactive
            pipe.zrem('jobs:recent', job_id)
            pipe.zrem(f"company:jobs:{job_data['company_id']}", job_id)
            if job_data.get('department'):
                pipe.zrem(f"dept:{self._normalize_text(job_data['department'])}", job_id)
            if job_data.get('location'):
                pipe.zrem(f"location:{self._normalize_location(job_data['location'])}", job_id)
            # Remove from search indices
            title_words = set(self._normalize_text(job_data['title']).split())
            for word in title_words:
                if len(word) > 2:
                    pipe.zrem(f"search:title:{word}", job_id)
        
        pipe.execute() 