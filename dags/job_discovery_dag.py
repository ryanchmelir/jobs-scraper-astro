"""
Job discovery DAG that quickly finds new job listings from company sources.

This DAG focuses solely on scraping company job listing pages to discover new jobs
as quickly as possible. It runs frequently with high concurrency to minimize the
time between a job being posted and us discovering it.

Scheduling Information:
- Runs every 10 minutes
- Does not perform catchup for missed intervals
- High SQL concurrency for fast discovery
- Uses 'scraping_bee' pool to limit to 5 concurrent requests across all DAGs
- Short timeout per task
"""
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
import httpx
import logging
import json

from airflow.decorators import dag, task, task_group
from airflow.models.baseoperator import chain
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException
from config.settings import SCRAPING_BEE_API_KEY
from infrastructure.models import SourceType
from sources.greenhouse import GreenhouseSource

# Default arguments for all tasks
default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=5),  # Short timeout since we're just scraping listings
    'email_on_failure': True,
    'email_on_retry': False,
    'depends_on_past': False,
}

@dag(
    dag_id='job_discovery',
    default_args=default_args,
    description='Quickly discovers new job listings from company sources',
    schedule_interval=timedelta(minutes=10),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['scraping', 'jobs', 'discovery'],
    max_active_tasks=30,  # Allow high concurrency for non-scraping tasks
    max_active_runs=3,
    dagrun_timeout=timedelta(minutes=30),
)
def job_discovery_dag():
    """Creates a DAG for discovering new job listings."""
    
    @task
    def get_company_sources_to_scrape(batch_size: int = 125) -> List[Dict]:
        """
        Selects company sources that are due for scraping using the get_sources_for_scraping function.
        Uses a larger batch size since we're just doing quick listing scrapes.
        """
        pg_hook = PostgresHook(postgres_conn_id='postgres_jobs_db')
        
        sources = pg_hook.get_records(
            "SELECT * FROM get_sources_for_scraping(%(batch_size)s)",
            parameters={'batch_size': batch_size}
        )
        
        return [
            {
                'id': source[0],
                'company_id': source[1],
                'source_type': source[2],
                'source_id': source[3],
                'config': source[4],
                'failure_count': source[5]
            }
            for source in sources
        ]

    @task(pool='scraping_bee', pool_slots=1, executor_config={"max_workers": 5})
    def scrape_listings(source: Dict) -> List[Dict]:
        """
        Quickly scrapes basic job listing information.
        Uses Airflow pool to ensure max 5 concurrent requests to ScrapingBee across all DAGs.
        Properly handles rate limiting and updates company_source_issues on failures.
        """
        if source['source_type'] == SourceType.GREENHOUSE.value:
            source_handler = GreenhouseSource()
        else:
            raise ValueError(f"Unsupported source type: {source['source_type']}")
        
        pg_hook = PostgresHook(postgres_conn_id='postgres_jobs_db')
        
        # Add logging for initial config state
        logging.info(f"Initial source config in scrape_listings: {json.dumps(source.get('config'))}")
        
        try:
            listings_url = source_handler.get_listings_url(source['source_id'], source.get('config', {}))
            scraping_config = source_handler.prepare_scraping_config(listings_url)
            
            logging.info(f"Scraping listings from {listings_url}")
            with httpx.Client(timeout=30.0) as client:
                response = client.get('https://app.scrapingbee.com/api/v1/', params=scraping_config)
                
                # Handle rate limiting explicitly
                if response.status_code == 429:
                    error_msg = "Rate limited by ScrapingBee (429)"
                    logging.warning(f"{error_msg} for source {source['id']}")
                    
                    # Update company_source_issues
                    pg_hook.run(
                        "SELECT track_source_issue(%(source_id)s, %(error)s)",
                        parameters={
                            'source_id': source['id'],
                            'error': error_msg
                        }
                    )
                    
                    # For rate limiting, we still want to retry
                    raise AirflowException(error_msg)
                
                try:
                    response.raise_for_status()
                except httpx.HTTPStatusError as e:
                    error_msg = str(e)
                    logging.error(f"Error scraping {listings_url}: {error_msg}")
                    
                    # Record the error but don't fail the task
                    pg_hook.run(
                        "SELECT track_source_issue(%(source_id)s, %(error)s)",
                        parameters={
                            'source_id': source['id'],
                            'error': error_msg
                        }
                    )
                    
                    # Return empty listings instead of failing
                    return []
                
                listings = source_handler.parse_listings_page(response.text, source['source_id'], source.get('config', {}))
                listings_dict = [
                    {
                        'source_job_id': listing.source_job_id,
                        'title': listing.title,
                        'location': listing.location,
                        'department': listing.department,
                        'url': listing.url,
                        'raw_data': listing.raw_data
                    }
                    for listing in listings
                ]
                
                # Update source config if needed
                if source.get('config') is None:
                    source['config'] = {}
                source['config']['working_url_pattern'] = scraping_config['url']
                
                # Clear any previous issues on success
                pg_hook.run(
                    "SELECT clear_source_issues(%(source_id)s)",
                    parameters={'source_id': source['id']}
                )
                
                # Update source with listing pattern config
                pg_hook.run(
                    "SELECT update_source_config(%(source_id)s, %(pattern)s)",
                    parameters={
                        'source_id': source['id'],
                        'pattern': source['config'].get('working_url_pattern')
                    }
                )
                
                logging.info(f"Found {len(listings_dict)} listings")
                return listings_dict
                
        except Exception as e:
            if isinstance(e, AirflowException):
                # Re-raise rate limiting exceptions for retry
                raise
            
            # Preserve failure logging
            error_msg = str(e)
            logging.error(f"Error scraping {listings_url}: {error_msg}")
            
            pg_hook.run(
                "SELECT track_source_issue(%(source_id)s, %(error)s)",
                parameters={
                    'source_id': source['id'],
                    'error': error_msg
                }
            )
            
            # Return empty listings instead of failing
            return []

    @task
    def process_listings(source_and_listings: Tuple[Dict, List[Dict]]) -> Dict[str, List[str]]:
        """Now properly receives (source, listings) tuple"""
        source, listings = source_and_listings
        pg_hook = PostgresHook(postgres_conn_id='postgres_jobs_db')
        
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    WITH existing AS (
                        SELECT source_job_id 
                        FROM jobs 
                        WHERE company_source_id = %(source_id)s 
                        AND active = true
                    )
                    SELECT ARRAY_AGG(source_job_id) FROM existing
                """, {'source_id': source['id']})
                
                result = cur.fetchone()
                existing_job_ids = set(result[0] or []) if result else set()
        
        scraped_job_ids = {listing['source_job_id'] for listing in listings}
        
        return {
            'new_jobs': list(scraped_job_ids - existing_job_ids),
            'removed_jobs': list(existing_job_ids - scraped_job_ids),
            'existing_jobs': list(scraped_job_ids & existing_job_ids)
        }

    @task(retries=3, retry_delay=timedelta(seconds=10), trigger_rule="all_done")
    def save_new_jobs(source_changes_and_listings) -> None:
        """Save jobs with direct URLs using batch operations"""
        source, job_changes, listings = source_changes_and_listings
        
        # Handle case where upstream mapped task failed
        if job_changes is None:
            logging.info(f"Skipping save_new_jobs for source {source['id']} - upstream task failed")
            return
        
        pg_hook = PostgresHook(postgres_conn_id='postgres_jobs_db')
        
        logging.info(f"Saving jobs for source {source['id']}: {len(job_changes['new_jobs'])} new, "
                    f"{len(job_changes['removed_jobs'])} removed, {len(job_changes['existing_jobs'])} existing")
        
        # Debug logging for input data
        logging.debug(f"Source data: {json.dumps(source, indent=2)}")
        logging.debug(f"Job changes: {json.dumps(job_changes, indent=2)}")
        logging.debug(f"Listings count: {len(listings)}")
        logging.debug(f"First listing sample: {json.dumps(listings[0] if listings else None, indent=2)}")
        
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                try:
                    # Mark removed jobs as inactive
                    if job_changes['removed_jobs']:
                        logging.info(f"Marking {len(job_changes['removed_jobs'])} jobs as inactive")
                        logging.debug(f"Jobs to mark inactive: {job_changes['removed_jobs']}")
                        cur.execute(
                            "SELECT mark_jobs_inactive(%(source_id)s, %(job_ids)s)",
                            {
                                'source_id': source['id'],
                                'job_ids': job_changes['removed_jobs']
                            }
                        )
                    
                    # Prepare and upsert active jobs
                    active_jobs = job_changes['new_jobs'] + job_changes['existing_jobs']
                    if active_jobs:
                        # Debug log active jobs
                        logging.debug(f"Active jobs to process: {active_jobs}")
                        
                        # Log each listing's source_job_id for verification
                        job_ids_in_listings = [listing.get('source_job_id') for listing in listings]
                        logging.debug(f"source_job_ids in listings: {job_ids_in_listings}")
                        
                        jobs_data = {
                            'company_source_id': source['id'],
                            'jobs': [
                                {
                                    'source_job_id': listing['source_job_id'],
                                    'title': listing['title'],
                                    'location': listing['location'],
                                    'department': listing['department'],
                                    'url': listing['url'],
                                    'raw_data': listing.get('raw_data', {})
                                }
                                for listing in listings
                                if listing['source_job_id'] in active_jobs
                            ]
                        }
                        
                        # Debug log the constructed jobs_data
                        logging.debug(f"Constructed jobs_data: {json.dumps(jobs_data, indent=2)}")
                        
                        # Debug log the exact SQL parameters
                        sql_params = {'jobs_data': json.dumps(jobs_data)}
                        logging.debug(f"SQL parameters: {json.dumps(sql_params, indent=2)}")
                        
                        cur.execute(
                            "SELECT * FROM upsert_jobs_batch(%(jobs_data)s)",
                            sql_params
                        )
                        
                        logging.info(f"Upserted {len(active_jobs)} jobs")
                    
                    conn.commit()
                    
                except Exception as e:
                    conn.rollback()
                    logging.error(f"Error saving jobs for source {source['id']}: {str(e)}")
                    # Log the full exception details
                    logging.error("Full exception details:", exc_info=True)
                    raise

    @task(trigger_rule="all_done")
    def update_source_status(source_and_changes_and_listings) -> None:
        """Updates source status with failure tracking"""
        source, job_changes, listings = source_and_changes_and_listings
        pg_hook = PostgresHook(postgres_conn_id='postgres_jobs_db')
        
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                try:
                    # Get the source's scrape interval
                    cur.execute("""
                        SELECT scrape_interval 
                        FROM company_sources 
                        WHERE id = %(source_id)s
                    """, {'source_id': source['id']})
                    
                    interval_minutes = cur.fetchone()[0] or 1440  # Default to 24 hours
                    
                    if listings:
                        # Success - update scrape time and clear issues
                        cur.execute(
                            "SELECT update_source_scrape_time(%(source_id)s, %(interval)s)",
                            {
                                'source_id': source['id'],
                                'interval': interval_minutes
                            }
                        )
                        
                        cur.execute(
                            "SELECT clear_source_issues(%(source_id)s)",
                            {'source_id': source['id']}
                        )
                    else:
                        # Track failure
                        cur.execute(
                            "SELECT track_source_issue(%(source_id)s, %(error)s)",
                            {
                                'source_id': source['id'],
                                'error': 'No listings found'
                            }
                        )
                    
                    conn.commit()
                    
                except Exception as e:
                    conn.rollback()
                    logging.error(f"Error updating source status: {str(e)}")
                    raise

    @task_group
    def process_single_source(source):
        # Get listings for the source
        listings = scrape_listings(source)
        
        # Pass complete context to downstream tasks
        job_changes = process_listings([source, listings])
        
        # Pass all context to final tasks
        saved = save_new_jobs([source, job_changes, listings])
        update_source_status([source, job_changes, listings])
        return saved

    process_single_source.expand(source=get_company_sources_to_scrape())

# Instantiate the DAG
job_discovery_dag() 