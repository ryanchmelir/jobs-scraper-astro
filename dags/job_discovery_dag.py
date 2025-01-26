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
from typing import List, Dict, Optional
import httpx
import logging
import json

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.providers.postgres.hooks.postgres import PostgresHook
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
    max_active_tasks=20,  # High SQL concurrency
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
        
        # Split sources into chunks of 5 to respect ScrapingBee's concurrent request limit
        chunked_sources = []
        for i in range(0, len(sources), 5):
            chunk = sources[i:i+5]
            chunked_sources.extend([
                {
                    'id': source[0],
                    'company_id': source[1],
                    'source_type': source[2],
                    'source_id': source[3],
                    'config': source[4],
                    'failure_count': source[5],
                    'chunk_id': i // 5  # Add chunk ID for potential use in rate limiting
                }
                for source in chunk
            ])
        
        return chunked_sources

    @task(pool='scraping_bee', pool_slots=1)
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
        
        try:
            listings_url = source_handler.get_listings_url(source['source_id'], source.get('config', {}))
            scraping_config = source_handler.prepare_scraping_config(listings_url)
            
            logging.info(f"Scraping listings from {listings_url} (chunk {source['chunk_id']})")
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
                    
                    from airflow.exceptions import AirflowException
                    raise AirflowException(error_msg)
                
                response.raise_for_status()
                
                listings = source_handler.parse_listings_page(response.text, source['source_id'])
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
                
                # Update source success timestamp
                pg_hook.run("""
                    UPDATE company_sources 
                    SET config = %(config)s,
                        next_scrape_time = NOW() + INTERVAL '10 minutes'
                    WHERE id = %(source_id)s
                """, parameters={
                    'source_id': source['id'],
                    'config': json.dumps(source['config'])
                })
                
                logging.info(f"Found {len(listings_dict)} listings")
                return listings_dict
                
        except Exception as e:
            error_msg = str(e)
            logging.error(f"Error scraping {listings_url}: {error_msg}")
            
            # Don't update issues table for rate limiting as it's handled above
            if not isinstance(e, AirflowException):
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
            
            raise  # Re-raise to trigger task failure

    @task
    def process_listings(source_and_listings) -> Dict[str, List[str]]:
        """Identifies new and existing jobs and validates URLs."""
        source, listings = source_and_listings
        pg_hook = PostgresHook(postgres_conn_id='postgres_jobs_db')
        
        # Get existing active jobs for this source
        sql = """
            SELECT source_job_id 
            FROM jobs 
            WHERE company_source_id = %(source_id)s 
            AND active = true
        """
        existing_jobs = pg_hook.get_records(sql, parameters={'source_id': source['id']})
        
        existing_job_ids = {job[0] for job in existing_jobs}
        scraped_job_ids = {listing['source_job_id'] for listing in listings}
        
        new_jobs = scraped_job_ids - existing_job_ids
        removed_jobs = existing_job_ids - scraped_job_ids
        existing_jobs = scraped_job_ids & existing_job_ids
        
        # Validate URLs for new jobs
        if source['source_type'] == SourceType.GREENHOUSE.value and new_jobs:
            source_handler = GreenhouseSource()
            working_patterns = set()
            redirecting_to_external = False
            
            # Test URL patterns with just one job to determine behavior
            sample_job_id = next(iter(new_jobs))
            sample_listing = next(l for l in listings if l['source_job_id'] == sample_job_id)
            
            try:
                # Try to validate the sample job's URL
                sample_listing['url'] = source_handler.get_job_detail_url(sample_listing, source.get('config', {}))
                
                # If we get here, we found a working pattern
                if pattern := sample_listing.get('raw_data', {}).get('config', {}).get('working_job_detail_pattern'):
                    working_patterns.add(pattern)
                    
                    # Apply the working pattern to all other new jobs
                    for listing in listings:
                        if listing['source_job_id'] in new_jobs and listing['source_job_id'] != sample_job_id:
                            try:
                                listing['url'] = pattern.format(
                                    company=source['source_id'],
                                    job_id=listing['source_job_id']
                                )
                            except Exception as e:
                                logging.warning(f"Failed to apply working pattern to job {listing['source_job_id']}: {str(e)}")
                                
            except ValueError as e:
                if "302 redirect" in str(e).lower():
                    # If we're getting redirects, assume all jobs redirect externally
                    redirecting_to_external = True
                    logging.info(f"Source {source['id']} appears to redirect job listings externally")
                    
                    # Update all listings with their redirect URLs
                    for listing in listings:
                        if listing['source_job_id'] in new_jobs:
                            try:
                                # Use the URL from the listing page since we can't get detail page
                                listing['url'] = listing.get('url', '')
                                if not listing.get('raw_data'):
                                    listing['raw_data'] = {}
                                listing['raw_data']['redirects_externally'] = True
                            except Exception as e:
                                logging.warning(f"Failed to process external URL for job {listing['source_job_id']}: {str(e)}")
                else:
                    logging.warning(f"Failed to validate URLs for source {source['id']}: {str(e)}")
            
            # Update source config with working patterns if found
            if working_patterns:
                if source.get('config') is None:
                    source['config'] = {}
                source['config']['working_job_detail_patterns'] = list(working_patterns)
            
            # If redirecting externally, update source config to indicate this
            if redirecting_to_external:
                if source.get('config') is None:
                    source['config'] = {}
                source['config']['redirects_externally'] = True
        
        logging.info(f"Source {source['id']}: {len(new_jobs)} new, "
                    f"{len(removed_jobs)} removed, {len(existing_jobs)} existing")
        
        return {
            'new_jobs': list(new_jobs),
            'removed_jobs': list(removed_jobs),
            'existing_jobs': list(existing_jobs),
            'redirects_externally': redirecting_to_external if source['source_type'] == SourceType.GREENHOUSE.value else False
        }

    @task
    def save_new_jobs(source_changes_and_listings) -> None:
        """Saves newly discovered jobs to the database."""
        source, job_changes, listings = source_changes_and_listings
        pg_hook = PostgresHook(postgres_conn_id='postgres_jobs_db')
        now = datetime.utcnow()
        
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                try:
                    # Mark removed jobs as inactive
                    if job_changes['removed_jobs']:
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
                    
                    # Update last_seen for existing jobs
                    if job_changes['existing_jobs']:
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
                    
                    # Insert new jobs with minimal data
                    if job_changes['new_jobs']:
                        new_job_listings = [
                            listing for listing in listings
                            if listing['source_job_id'] in job_changes['new_jobs']
                        ]
                        
                        for listing in new_job_listings:
                            # Don't mark jobs as needing details if they redirect externally
                            needs_details = not (
                                job_changes.get('redirects_externally', False) or
                                listing.get('raw_data', {}).get('redirects_externally', False)
                            )
                            
                            cur.execute("""
                                INSERT INTO jobs (
                                    company_id,
                                    title,
                                    location,
                                    department,
                                    url,
                                    raw_data,
                                    active,
                                    first_seen,
                                    last_seen,
                                    created_at,
                                    updated_at,
                                    company_source_id,
                                    source_job_id,
                                    needs_details
                                ) VALUES (
                                    %(company_id)s,
                                    %(title)s,
                                    %(location)s,
                                    %(department)s,
                                    %(url)s,
                                    %(raw_data)s,
                                    true,
                                    %(now)s,
                                    %(now)s,
                                    %(now)s,
                                    %(now)s,
                                    %(source_id)s,
                                    %(source_job_id)s,
                                    %(needs_details)s
                                )
                                ON CONFLICT (company_source_id, source_job_id) DO UPDATE
                                SET active = true,
                                    last_seen = EXCLUDED.last_seen,
                                    updated_at = EXCLUDED.updated_at,
                                    needs_details = EXCLUDED.needs_details
                            """, {
                                'company_id': source['company_id'],
                                'title': listing['title'],
                                'location': listing['location'],
                                'department': listing['department'],
                                'url': listing['url'],
                                'raw_data': json.dumps(listing['raw_data']),
                                'now': now,
                                'source_id': source['id'],
                                'source_job_id': listing['source_job_id'],
                                'needs_details': needs_details
                            })
                    
                    conn.commit()
                    
                except Exception as e:
                    conn.rollback()
                    logging.error(f"Error saving jobs: {str(e)}")
                    raise

    @task
    def update_source_status(source_and_listings) -> None:
        """Updates source success/failure status and next scrape time."""
        source, listings = source_and_listings
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
                    
                    # Update source status and config
                    if listings:
                        # Success - clear any failure records and update config
                        cur.execute("""
                            UPDATE company_sources 
                            SET last_scraped = %(now)s,
                                next_scrape_time = %(next_scrape)s,
                                config = CASE 
                                    WHEN (%(config)s::json->>'working_job_detail_patterns') IS NOT NULL
                                    THEN json_build_object(
                                        'working_job_detail_patterns', 
                                        %(patterns)s::json
                                    )::json
                                    ELSE COALESCE(config, '{}'::json)
                                END
                            WHERE id = %(source_id)s
                        """, {
                            'source_id': source['id'],
                            'now': now,
                            'next_scrape': next_scrape,
                            'config': json.dumps(source.get('config', {})),
                            'patterns': json.dumps(source.get('config', {}).get('working_job_detail_patterns', []))
                        })
                        
                        cur.execute("""
                            DELETE FROM company_source_issues
                            WHERE company_source_id = %(source_id)s
                        """, {'source_id': source['id']})
                    else:
                        # Failure - increment failure count
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

    # Get sources to scrape
    sources = get_company_sources_to_scrape()
    
    # Map scraping task to each source
    listings = scrape_listings.expand(source=sources)
    
    # Process listings for each source
    job_changes = process_listings.expand(
        source_and_listings=sources.zip(listings)
    )
    
    # Save new jobs and update source status
    save_jobs = save_new_jobs.expand(
        source_changes_and_listings=sources.zip(job_changes, listings)
    )
    
    source_updates = update_source_status.expand(
        source_and_listings=sources.zip(listings)
    )
    
    # Set up dependencies
    chain(
        listings,
        job_changes,
        save_jobs,
        source_updates
    )

# Instantiate the DAG
job_discovery_dag() 