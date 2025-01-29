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
from typing import List, Dict, Optional, tuple
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
    max_active_tasks=50,  # Allow high concurrency for non-scraping tasks
    max_active_runs=3,
    dagrun_timeout=timedelta(minutes=30),
)
def job_discovery_dag():
    """Creates a DAG for discovering new job listings."""
    
    @task
    def get_company_sources_to_scrape(batch_size: int = 50) -> List[Dict]:
        """
        Selects company sources that are due for scraping.
        Uses a larger batch size since we're just doing quick listing scrapes.
        """
        pg_hook = PostgresHook(postgres_conn_id='postgres_jobs_db')
        
        sql = """
            SELECT 
                cs.id,
                cs.company_id,
                cs.source_type,
                cs.source_id,
                cs.config,
                COALESCE(csi.failure_count, 0) as failure_count
            FROM company_sources cs
            LEFT JOIN company_source_issues csi ON cs.id = csi.company_source_id
            WHERE cs.active = true 
            AND (cs.next_scrape_time <= NOW() OR cs.next_scrape_time IS NULL)
            AND (csi.failure_count IS NULL OR csi.failure_count < 3)
            ORDER BY cs.next_scrape_time ASC
            LIMIT %(batch_size)s
            FOR UPDATE OF cs SKIP LOCKED
        """
        sources = pg_hook.get_records(sql, parameters={'batch_size': batch_size})
        
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
                    pg_hook.run("""
                        INSERT INTO company_source_issues (company_source_id, failure_count, last_failure, last_error)
                        VALUES (%(source_id)s, 1, NOW(), %(error)s)
                        ON CONFLICT (company_source_id) DO UPDATE SET
                            failure_count = company_source_issues.failure_count + 1,
                            last_failure = NOW(),
                            last_error = %(error)s
                    """, parameters={
                        'source_id': source['id'],
                        'error': error_msg
                    })
                    
                    # For rate limiting, we still want to retry
                    raise AirflowException(error_msg)
                
                try:
                    response.raise_for_status()
                except httpx.HTTPStatusError as e:
                    error_msg = str(e)
                    logging.error(f"Error scraping {listings_url}: {error_msg}")
                    
                    # Record the error but don't fail the task
                    pg_hook.run("""
                        INSERT INTO company_source_issues (company_source_id, failure_count, last_failure, last_error)
                        VALUES (%(source_id)s, 1, NOW(), %(error)s)
                        ON CONFLICT (company_source_id) DO UPDATE SET
                            failure_count = company_source_issues.failure_count + 1,
                            last_failure = NOW(),
                            last_error = %(error)s
                    """, parameters={
                        'source_id': source['id'],
                        'error': error_msg
                    })
                    
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
                pg_hook.run("""
                    DELETE FROM company_source_issues
                    WHERE company_source_id = %(source_id)s
                """, parameters={'source_id': source['id']})
                
                # Update source with listing pattern config
                pg_hook.run("""
                    UPDATE company_sources 
                    SET config = json_build_object(
                        'working_url_pattern', 
                        COALESCE(%(pattern)s, config->>'working_url_pattern')
                    ),
                    next_scrape_time = NOW() + INTERVAL '10 minutes'
                    WHERE id = %(source_id)s
                """, parameters={
                    'source_id': source['id'],
                    'pattern': source['config'].get('working_url_pattern')
                })
                
                logging.info(f"Found {len(listings_dict)} listings")
                return listings_dict
                
        except Exception as e:
            if isinstance(e, AirflowException):
                # Re-raise rate limiting exceptions for retry
                raise
            
            # Preserve failure logging
            error_msg = str(e)
            logging.error(f"Error scraping {listings_url}: {error_msg}")
            
            pg_hook.run("""
                INSERT INTO company_source_issues (company_source_id, failure_count, last_failure, last_error)
                VALUES (%(source_id)s, 1, NOW(), %(error)s)
                ON CONFLICT (company_source_id) DO UPDATE SET
                    failure_count = company_source_issues.failure_count + 1,
                    last_failure = NOW(),
                    last_error = %(error)s
            """, parameters={
                'source_id': source['id'],
                'error': error_msg
            })
            
            # Return empty listings instead of failing
            return []

    @task
    def process_listings(source_and_listings: tuple[Dict, List[Dict]]) -> Dict[str, List[str]]:
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
        """Save jobs with direct URLs"""
        from psycopg2.extras import execute_batch
        from psycopg2 import OperationalError
        from time import sleep
        source, job_changes, listings = source_changes_and_listings
        pg_hook = PostgresHook(postgres_conn_id='postgres_jobs_db')
        now = datetime.utcnow()
        
        logging.info(f"Saving jobs for source {source['id']}: {len(job_changes['new_jobs'])} new, "
                    f"{len(job_changes['removed_jobs'])} removed, {len(job_changes['existing_jobs'])} existing")
        
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                for attempt in range(3):
                    try:
                        # Mark removed jobs as inactive
                        if job_changes['removed_jobs']:
                            logging.info(f"Marking {len(job_changes['removed_jobs'])} jobs as inactive")
                            cur.execute("""
                                UPDATE jobs 
                                SET active = false,
                                    updated_at = %(now)s
                                WHERE company_source_id = %(source_id)s
                                AND source_job_id = ANY(%(job_ids)s)
                            """, {
                                'source_id': source['id'],
                                'job_ids': job_changes['removed_jobs'],
                                'now': now
                            })
                            logging.info(f"Updated {cur.rowcount} removed jobs")
                        
                        # Update last_seen for existing jobs
                        if job_changes['existing_jobs']:
                            logging.info(f"Updating {len(job_changes['existing_jobs'])} existing jobs")
                            cur.execute("""
                                UPDATE jobs 
                                SET last_seen = %(now)s,
                                    updated_at = %(now)s
                                WHERE company_source_id = %(source_id)s
                                AND source_job_id = ANY(%(job_ids)s)
                            """, {
                                'source_id': source['id'],
                                'job_ids': job_changes['existing_jobs'],
                                'now': now
                            })
                            logging.info(f"Updated {cur.rowcount} existing jobs")
                        
                        # Insert new jobs with minimal data
                        if job_changes['new_jobs']:
                            new_job_listings = [listing for listing in listings
                                              if listing['source_job_id'] in job_changes['new_jobs']]
                            
                            logging.info(f"Inserting {len(new_job_listings)} new jobs")
                            
                            insert_params = [{
                                'company_id': source['company_id'],
                                'title': listing['title'],
                                'location': listing['location'],
                                'department': listing['department'],
                                'url': listing['url'],
                                'raw_data': json.dumps(listing.get('raw_data', {})),
                                'now': now,
                                'source_id': source['id'],
                                'source_job_id': listing['source_job_id']
                            } for listing in new_job_listings]
                            
                            execute_batch(cur, """
                                INSERT INTO jobs (
                                    company_id, title, location, department, 
                                    url, raw_data, active, first_seen, 
                                    last_seen, created_at, updated_at, 
                                    company_source_id, source_job_id, needs_details
                                ) VALUES (
                                    %(company_id)s, %(title)s, %(location)s,
                                    %(department)s, %(url)s, %(raw_data)s, true,
                                    %(now)s, %(now)s, %(now)s, %(now)s,
                                    %(source_id)s, %(source_job_id)s, false
                                )
                                ON CONFLICT (company_source_id, source_job_id) DO UPDATE SET
                                    active = EXCLUDED.active,
                                    last_seen = EXCLUDED.last_seen,
                                    updated_at = EXCLUDED.updated_at,
                                    needs_details = EXCLUDED.needs_details
                            """, insert_params, page_size=100)
                            
                            logging.info(f"Bulk inserted/updated {len(new_job_listings)} jobs")
                        
                        conn.commit()
                        logging.info("Successfully committed all job changes to database")
                        break
                        
                    except OperationalError as e:
                        if 'deadlock' in str(e).lower() and attempt < 2:
                            logging.warning(f"Deadlock detected, retrying (attempt {attempt+1})")
                            conn.rollback()
                            sleep(0.1 * (attempt + 1))
                            continue
                        else:
                            logging.error("Persistent database error")
                            return  # Don't raise to prevent DAG failure
                            
                    except Exception as e:
                        conn.rollback()
                        logging.error(f"Error saving jobs: {str(e)}")
                        logging.error(f"Failed SQL parameters: {cur.query.decode()}")
                        return  # Don't raise to prevent DAG failure

    @task(trigger_rule="all_done")
    def update_source_status(source_and_changes_and_listings) -> None:
        """Updates source status with failure tracking"""
        source, job_changes, listings = source_and_changes_and_listings
        pg_hook = PostgresHook(postgres_conn_id='postgres_jobs_db')
        now = datetime.utcnow()
        
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
                    next_scrape = now + timedelta(minutes=interval_minutes)
                    
                    if listings:
                        # Success - clear any failure records and update config
                        cur.execute("""
                            UPDATE company_sources 
                            SET last_scraped = %(now)s,
                                next_scrape_time = %(next_scrape)s,
                                config = json_build_object(
                                    'working_url_pattern', 
                                    COALESCE(
                                        config->>'working_url_pattern',
                                        '{{ cookiecutter.default_listing_pattern }}'
                                    )
                                )
                            WHERE id = %(source_id)s
                        """, {
                            'source_id': source['id'],
                            'now': now,
                            'next_scrape': next_scrape
                        })
                        
                        cur.execute("""
                            DELETE FROM company_source_issues
                            WHERE company_source_id = %(source_id)s
                        """, {'source_id': source['id']})
                    else:
                        # Preserve failure increment
                        cur.execute("""
                            INSERT INTO company_source_issues (company_source_id, failure_count, last_failure)
                            VALUES (%(source_id)s, 1, NOW())
                            ON CONFLICT (company_source_id) 
                            DO UPDATE SET 
                                failure_count = company_source_issues.failure_count + 1,
                                last_failure = NOW()
                        """, {'source_id': source['id']})
                    
                    conn.commit()
                    
                except Exception as e:
                    conn.rollback()
                    logging.error(f"Error updating source status: {str(e)}")
                    raise

    @task_group
    def process_single_source(source):
        listings = scrape_listings(source)
        # Zip source with its listings
        source_with_listings = source.zip(listings)
        job_changes = process_listings(source_with_listings)
        # Zip all context for downstream
        full_context = source.zip(job_changes, listings)
        saved = save_new_jobs(full_context)
        update_source_status(full_context)
        return saved

    process_single_source.expand(source=get_company_sources_to_scrape())

# Instantiate the DAG
job_discovery_dag() 