"""
Redis sync DAG that maintains a Redis cache of job listings and companies.
Supports both incremental (every 5 minutes) and full (daily) syncs.

The DAG maintains complex Redis data structures including:
- Recent jobs (sorted set)
- Company-specific job listings
- Department-based searches
- Location-based searches
- Title keyword searches

All data structures have appropriate TTL values:
- Recent jobs: 1 hour
- Job details: 6 hours
- Company details: 24 hours
- Search indices: 2 hours
"""
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Type
import logging
import json
import traceback
from contextlib import contextmanager
import time

from airflow.decorators import dag, task, task_group
from airflow.models.baseoperator import chain
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.exceptions import AirflowException

from infrastructure.redis_sync import RedisCache
from infrastructure.models import SourceType
import redis

# Configure logging
logger = logging.getLogger(__name__)

# Default arguments for all tasks
default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=30),
    'email_on_failure': True,
    'email_on_retry': False,
    'depends_on_past': False,
}

@contextmanager
def log_operation_time(operation_name: str):
    """Context manager to log operation timing."""
    start_time = time.time()
    try:
        yield
    finally:
        duration = time.time() - start_time
        logger.info(f"{operation_name} took {duration:.2f} seconds")

def get_redis_connection() -> RedisCache:
    """Get Redis connection with minimal, essential configuration."""
    try:
        conn = BaseHook.get_connection('redis_cache')
        
        redis_config = {
            'host': conn.host,
            'port': conn.port,
            'decode_responses': True,
            'socket_timeout': 30,
            'socket_connect_timeout': 30,
            'retry_on_timeout': True
        }
        
        logger.info(f"Initializing Redis connection with config: {json.dumps(redis_config)}")
        return RedisCache(**redis_config)
    except Exception as e:
        logger.error("Failed to initialize Redis connection")
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Error message: {str(e)}")
        raise AirflowException(f"Redis connection failed: {str(e)}")

@dag(
    dag_id='redis_sync',
    default_args=default_args,
    description='Syncs job listings and companies to Redis cache',
    schedule_interval=timedelta(minutes=5),  # Run every 5 minutes for incremental syncs
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['redis', 'sync', 'cache'],
    max_active_tasks=5,  # Limit concurrent tasks
    dagrun_timeout=timedelta(minutes=60),
)
def redis_sync_dag():
    """Creates a DAG for syncing data to Redis cache."""
    
    @task
    def get_sync_type(**context) -> Dict[str, bool]:
        """Determine if this should be a full sync based on time of day."""
        execution_date = context['logical_date']
        
        # Do full sync at 2 AM every day
        is_full_sync = (
            execution_date.hour == 2 and 
            execution_date.minute < 5  # Within first 5-minute window of 2 AM
        )
        
        logger.info(f"Sync type determined: {'full' if is_full_sync else 'incremental'} sync")
        return {'full_sync': is_full_sync}

    @task_group(group_id='sync_companies')
    def sync_companies():
        """Task group for syncing companies to Redis."""
        
        @task
        def get_active_companies() -> List[Dict]:
            """Get all active companies from PostgreSQL."""
            with log_operation_time("get_active_companies"):
                pg_hook = PostgresHook(postgres_conn_id='postgres_jobs_db')
                
                sql = """
                    SELECT id, name, active
                    FROM companies
                    WHERE active = true
                """
                
                companies = []
                for row in pg_hook.get_records(sql):
                    companies.append({
                        'id': row[0],
                        'name': row[1],
                        'active': row[2]
                    })
                
                logger.info(f"Found {len(companies)} active companies")
                logger.debug(f"Company IDs to sync: {[c['id'] for c in companies]}")
                return companies
        
        @task
        def sync_companies_to_redis(companies: List[Dict]) -> None:
            """Sync companies to Redis cache."""
            logger.info(f"Starting Redis sync for {len(companies)} companies")
            
            try:
                cache = get_redis_connection()
                logger.info("Successfully initialized Redis connection")
                
                # Sync all companies in one operation
                cache.sync_companies_batch(companies)
                logger.info(f"Successfully synced {len(companies)} companies")
                
            except Exception as e:
                logger.error("Failed to sync companies")
                logger.error(f"Error type: {type(e).__name__}")
                logger.error(f"Error message: {str(e)}")
                logger.error(f"Stack trace:\n{''.join(traceback.format_tb(e.__traceback__))}")
                raise AirflowException(f"Company sync failed: {str(e)}")
        
        # Chain company sync tasks
        sync_companies_to_redis(get_active_companies())
    
    @task_group(group_id='sync_jobs')
    def sync_jobs(sync_config: Dict[str, bool]):
        """Task group for syncing jobs to Redis."""
        
        @task
        def get_jobs_to_sync(full_sync: bool) -> List[Dict]:
            """Get jobs that need to be synced from PostgreSQL."""
            pg_hook = PostgresHook(postgres_conn_id='postgres_jobs_db')
            
            if full_sync:
                sql = """
                    SELECT 
                        id, company_id, title, location, department,
                        url, active, first_seen
                    FROM jobs
                    WHERE active = true
                    ORDER BY id
                """
            else:
                sql = """
                    SELECT 
                        id, company_id, title, location, department,
                        url, active, first_seen
                    FROM jobs
                    WHERE updated_at >= NOW() - INTERVAL '1 hour'
                    ORDER BY id
                """
            
            jobs = []
            batch_size = 1000
            
            for row in pg_hook.get_records(sql):
                jobs.append({
                    'id': row[0],
                    'company_id': row[1],
                    'title': row[2],
                    'location': row[3],
                    'department': row[4],
                    'url': row[5],
                    'active': row[6],
                    'first_seen': row[7]
                })
                
                # Process in batches to avoid memory issues
                if len(jobs) >= batch_size:
                    yield jobs
                    jobs = []
            
            if jobs:  # Don't forget the last batch
                yield jobs
        
        @task
        def sync_jobs_to_redis(jobs_batch: List[Dict]) -> None:
            """Sync a batch of jobs to Redis cache."""
            logger.info(f"Starting sync of {len(jobs_batch)} jobs")
            cache = get_redis_connection()
            
            pipe = cache.redis.pipeline()
            for job in jobs_batch:
                try:
                    cache.sync_job(job)
                except Exception as e:
                    logger.error(f"Failed to sync job {job['id']}: {str(e)}")
                    raise
            
            logger.info(f"Successfully synced {len(jobs_batch)} jobs")
        
        # Get jobs and sync them in batches
        jobs_batches = get_jobs_to_sync(sync_config['full_sync'])
        sync_jobs_to_redis.expand(jobs_batch=jobs_batches)
    
    # Set up task dependencies
    sync_type = get_sync_type()
    companies_sync = sync_companies()
    jobs_sync = sync_jobs(sync_type)
    
    # Companies should sync before jobs
    chain(sync_type, companies_sync, jobs_sync)

# Create the DAG
dag = redis_sync_dag() 