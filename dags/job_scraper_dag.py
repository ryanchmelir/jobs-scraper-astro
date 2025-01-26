"""
Job scraper DAG that orchestrates the scraping of job listings from various sources.

This DAG follows a master DAG pattern with dynamic task mapping for scalability.
It handles the selection of company sources to scrape, fetches job listings,
processes them, and updates the database accordingly.

Scheduling Information:
- Runs hourly
- Does not perform catchup for missed intervals
- Only allows one active run at a time
- Has a 30-minute timeout per task
- Retries failed tasks up to 3 times with 5-minute delays
"""
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
import httpx
import logging
import json
import re

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain, cross_downstream
from airflow.providers.postgres.hooks.postgres import PostgresHook
from config.settings import SCRAPING_BEE_API_KEY
from infrastructure.models import SourceType, EmploymentType, RemoteStatus
from sources.greenhouse import GreenhouseSource

print("Basic imports successful")
print("Custom module imports successful")

# Default arguments for all tasks
default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=30),
    'email_on_failure': True,
    'email_on_retry': False,
    'depends_on_past': False,  # Prevent failed runs from blocking next runs
}

def parse_salary_string(salary_str: Optional[str]) -> Tuple[Optional[int], Optional[int], Optional[str]]:
    """Parse a salary string into min, max, and currency."""
    if not salary_str:
        return None, None, None
        
    # Remove any whitespace and convert to lowercase
    salary_str = salary_str.lower().strip()
    
    # Try to identify currency
    currency = 'USD'  # Default
    if '€' in salary_str:
        currency = 'EUR'
    elif '£' in salary_str:
        currency = 'GBP'
    
    # Extract numbers
    numbers = re.findall(r'(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)', salary_str)
    if not numbers:
        return None, None, None
        
    # Convert numbers to integers
    clean_numbers = []
    for num in numbers:
        # Remove commas and handle 'k' multiplier
        num = num.replace(',', '')
        if 'k' in salary_str:
            num = float(num) * 1000
        clean_numbers.append(int(float(num)))
    
    if len(clean_numbers) == 1:
        return clean_numbers[0], clean_numbers[0], currency
    elif len(clean_numbers) >= 2:
        return min(clean_numbers), max(clean_numbers), currency
    
    return None, None, currency

def normalize_employment_type(raw_type: Optional[str]) -> str:
    """Convert raw employment type string to enum value."""
    if not raw_type:
        return EmploymentType.UNKNOWN.name
        
    raw_type = raw_type.lower()
    
    if 'full' in raw_type and 'time' in raw_type:
        return EmploymentType.FULL_TIME.name
    elif 'part' in raw_type and 'time' in raw_type:
        return EmploymentType.PART_TIME.name
    elif 'contract' in raw_type:
        return EmploymentType.CONTRACT.name
    elif 'intern' in raw_type:
        return EmploymentType.INTERNSHIP.name
    elif 'temp' in raw_type:
        return EmploymentType.TEMPORARY.name
        
    return EmploymentType.UNKNOWN.name

def normalize_remote_status(raw_status: Optional[str]) -> str:
    """Convert raw remote status string to enum value."""
    if not raw_status:
        return RemoteStatus.UNKNOWN.name
        
    raw_status = raw_status.lower()
    
    if 'remote' in raw_status:
        if 'hybrid' in raw_status:
            return RemoteStatus.HYBRID.name
        return RemoteStatus.REMOTE.name
    elif 'hybrid' in raw_status:
        return RemoteStatus.HYBRID.name
    elif 'office' in raw_status or 'on-site' in raw_status or 'onsite' in raw_status:
        return RemoteStatus.OFFICE.name
        
    return RemoteStatus.UNKNOWN.name

@dag(
    dag_id='job_scraper',
    default_args=default_args,
    description='Scrapes job listings from configured company sources',
    schedule_interval=timedelta(minutes=10),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['scraping', 'jobs'],
    max_active_tasks=5,  # Limit concurrent tasks
    max_active_runs=3,   # Control concurrent DAG runs
    dagrun_timeout=timedelta(hours=1),  # Entire DAG must complete within 1 hour
)
def job_scraper_dag():
    """Creates a DAG for scraping job listings."""
    
    @task
    def get_company_sources_to_scrape(batch_size: int = 5) -> List[Dict]:
        """
        Selects company sources that are due for scraping and haven't failed too many times.
        
        Args:
            batch_size: Maximum number of sources to scrape in one run.
            
        Returns:
            List of company source records.
        """
        pg_hook = PostgresHook(postgres_conn_id='postgres_jobs_db')
        
        # Query for active sources that are due for scraping and haven't failed too many times
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
        
        # Convert to dictionaries
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

    @task
    def scrape_listings(source: Dict) -> List[Dict]:
        """
        Scrapes job listings for a single company source.
        
        Args:
            source: Company source record.
            
        Returns:
            List of job listings as dictionaries.
        """
        # Initialize the appropriate source handler
        if source['source_type'] == SourceType.GREENHOUSE.value:
            source_handler = GreenhouseSource()
        else:
            raise ValueError(f"Unsupported source type: {source['source_type']}")
        
        # Get the listings URL and scraping config
        listings_url = source_handler.get_listings_url(source['source_id'], source.get('config', {}))
        scraping_config = source_handler.prepare_scraping_config(listings_url)
        
        try:
            # Make the request to ScrapingBee
            logging.info(f"Scraping listings from {listings_url}")
            with httpx.Client(timeout=30.0) as client:
                response = client.get('https://app.scrapingbee.com/api/v1/', params=scraping_config)
                response.raise_for_status()
                
                # Parse the listings page and convert to dictionaries
                listings = source_handler.parse_listings_page(response.text, source['source_id'])
                listings_dict = [
                    {
                        'id': listing.source_job_id,
                        'source_job_id': listing.source_job_id,
                        'title': listing.title,
                        'location': listing.location,
                        'department': listing.department,
                        'url': listing.url,  # Use the original URL from parse_listings_page
                        'raw_data': listing.raw_data
                    }
                    for listing in listings
                ]
                
                # If successful, update the config with working URL pattern
                if source.get('config') is None:
                    source['config'] = {}
                source['config']['working_url_pattern'] = scraping_config['url']
                
                # Update the source config in the database
                pg_hook = PostgresHook(postgres_conn_id='postgres_jobs_db')
                pg_hook.run("""
                    UPDATE company_sources 
                    SET config = %(config)s
                    WHERE id = %(source_id)s
                """, parameters={
                    'source_id': source['id'],
                    'config': json.dumps(source['config'])
                })
                
                logging.info(f"Found {len(listings_dict)} listings")
                return listings_dict
                
        except httpx.HTTPError as e:
            logging.error(f"HTTP error while scraping {listings_url}: {str(e)}")
            raise
        except Exception as e:
            logging.error(f"Error while scraping {listings_url}: {str(e)}")
            raise

    @task
    def process_listings(source_and_listings) -> Dict[str, List[str]]:
        """
        Processes scraped listings to identify new, existing, and removed jobs.
        
        Args:
            source_and_listings: Tuple of (source, listings) from upstream tasks.
            
        Returns:
            Dictionary with lists of job IDs for new, existing, and removed jobs.
        """
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
        
        # Create sets for comparison
        existing_job_ids = {job[0] for job in existing_jobs}
        scraped_job_ids = {listing['source_job_id'] for listing in listings}  # Use source_job_id consistently
        
        # Identify changes
        new_jobs = scraped_job_ids - existing_job_ids
        removed_jobs = existing_job_ids - scraped_job_ids
        existing_jobs = scraped_job_ids & existing_job_ids
        
        # Log the changes
        logging.info(f"Source {source['id']}: {len(new_jobs)} new, "
                    f"{len(removed_jobs)} removed, {len(existing_jobs)} existing")
        
        return {
            'new_jobs': list(new_jobs),
            'removed_jobs': list(removed_jobs),
            'existing_jobs': list(existing_jobs)
        }

    @task
    def handle_new_jobs(source_changes_and_listings) -> List[Dict]:
        """
        Scrapes details for new jobs and prepares them for database insertion.
        
        Args:
            source_changes_and_listings: Tuple of (source, job_changes, listings) from upstream tasks.
            
        Returns:
            List of job listings with full details as dictionaries.
        """
        source, job_changes, listings = source_changes_and_listings
        
        if not job_changes['new_jobs']:
            logging.info(f"No new jobs to process for source {source['id']}")
            return []
            
        # Initialize source handler
        if source['source_type'] == SourceType.GREENHOUSE.value:
            source_handler = GreenhouseSource()
        else:
            raise ValueError(f"Unsupported source type: {source['source_type']}")
            
        # Get new job listings
        new_job_listings = [
            listing for listing in listings
            if listing['id'] in job_changes['new_jobs']
        ]
        
        detailed_jobs = []
        config_updated = False
        failed_jobs = []
        MAX_CONSECUTIVE_FAILURES = 3  # Stop after this many consecutive failures
        consecutive_failures = 0
        
        # Scrape details for each new job
        for listing in new_job_listings:
            try:
                # Check if we've hit too many consecutive failures
                if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                    logging.error(f"Stopping after {MAX_CONSECUTIVE_FAILURES} consecutive failures")
                    break
                    
                # Get job detail URL
                detail_url = source_handler.get_job_detail_url(listing, source.get('config', {}))
                
                # Make the request
                logging.info(f"Scraping job details from {detail_url}")
                with httpx.Client(timeout=30.0) as client:
                    response = client.get('https://app.scrapingbee.com/api/v1/', 
                                       params=source_handler.prepare_scraping_config(detail_url))
                    response.raise_for_status()
                    
                    # Parse job details
                    job_details = source_handler.parse_job_details(response.text, listing)
                    if not isinstance(job_details, dict):
                        raise ValueError(f"Expected dictionary from parse_job_details, got {type(job_details)}")
                    
                    # Reset consecutive failures on success
                    consecutive_failures = 0
                    
                    # Process the successful response
                    raw_data = job_details.get('raw_data', {})
                    if not isinstance(raw_data, dict):
                        raw_data = {}
                    
                    # Update config if we found a working pattern
                    if raw_data.get('config', {}).get('working_job_detail_pattern'):
                        config_updated = True
                        if source.get('config') is None:
                            source['config'] = {}
                        source['config']['working_job_detail_pattern'] = raw_data['config']['working_job_detail_pattern']
                    
                    # Update failed patterns if any
                    if raw_data.get('config', {}).get('failed_job_detail_patterns'):
                        if source.get('config') is None:
                            source['config'] = {}
                        source['config']['failed_job_detail_patterns'] = raw_data['config']['failed_job_detail_patterns']
                    
                    # Process the job details
                    salary_min, salary_max, salary_currency = parse_salary_string(raw_data.get('salary'))
                    employment_type = normalize_employment_type(raw_data.get('employment_type'))
                    remote_status = normalize_remote_status(raw_data.get('remote_status'))
                    
                    detailed_job = {
                        **listing,
                        'description': job_details.get('description', ''),
                        'url': job_details.get('url', listing.get('url', '')),
                        'salary_min': salary_min,
                        'salary_max': salary_max,
                        'salary_currency': salary_currency,
                        'employment_type': employment_type,
                        'remote_status': remote_status,
                        'raw_data': raw_data
                    }
                    detailed_jobs.append(detailed_job)
                    
            except Exception as e:
                logging.error(f"Error scraping job details for {listing['id']}: {str(e)}", exc_info=True)
                failed_jobs.append(listing['id'])
                consecutive_failures += 1
                continue
        
        # If we found a working pattern or failed patterns, update the source config
        if config_updated or source.get('config', {}).get('failed_job_detail_patterns'):
            pg_hook = PostgresHook(postgres_conn_id='postgres_jobs_db')
            pg_hook.run("""
                UPDATE company_sources 
                SET config = %(config)s
                WHERE id = %(source_id)s
            """, parameters={
                'source_id': source['id'],
                'config': json.dumps(source['config'])
            })
        
        if failed_jobs:
            logging.warning(f"Failed to scrape details for jobs: {failed_jobs}")
        
        logging.info(f"Scraped details for {len(detailed_jobs)} new jobs")
        return detailed_jobs

    @task
    def update_database(source_changes_and_jobs) -> None:
        """Updates the database with job changes."""
        source, job_changes, new_jobs = source_changes_and_jobs
        pg_hook = PostgresHook(postgres_conn_id='postgres_jobs_db')
        now = datetime.utcnow()
        
        # Define stale threshold - jobs not seen for this long will be marked inactive
        STALE_THRESHOLD = timedelta(days=7)
        
        # Start a transaction and acquire locks
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                try:
                    # Lock the company_source row to prevent concurrent updates to the same source
                    cur.execute("""
                        SELECT id 
                        FROM company_sources 
                        WHERE id = %(source_id)s 
                        FOR UPDATE
                    """, {'source_id': source['id']})
                    
                    # Mark jobs as inactive if they haven't been seen recently
                    cur.execute("""
                        UPDATE jobs 
                        SET active = false, 
                            updated_at = %(now)s
                        WHERE company_source_id = %(source_id)s
                        AND active = true
                        AND source_job_id != ALL(%(current_job_ids)s)
                        AND (%(now)s - last_seen) > %(stale_threshold)s
                    """, {
                        'source_id': source['id'],
                        'current_job_ids': [job['source_job_id'] for job in new_jobs],
                        'now': now,
                        'stale_threshold': STALE_THRESHOLD
                    })
                    
                    # Update last_seen for existing jobs
                    if new_jobs:
                        cur.execute("""
                            UPDATE jobs 
                            SET last_seen = %(now)s,
                                updated_at = %(now)s
                            WHERE company_source_id = %(source_id)s
                            AND source_job_id = ANY(%(job_ids)s)
                        """, {
                            'source_id': source['id'],
                            'job_ids': [job['source_job_id'] for job in new_jobs],
                            'now': now
                        })
                    
                    # Insert new jobs using ON CONFLICT DO UPDATE
                    if job_changes['new_jobs']:
                        # Define target fields in exact database column order
                        target_fields = [
                            'company_id',
                            'title',
                            'location',
                            'department',
                            'description',
                            'url',
                            'raw_data',
                            'active',
                            'first_seen',
                            'last_seen',
                            'created_at',
                            'updated_at',
                            'company_source_id',
                            'source_job_id',
                            'salary_min',
                            'salary_max',
                            'salary_currency',
                            'employment_type',
                            'remote_status'
                        ]
                        
                        # Convert job dictionaries to tuples
                        job_tuples = []
                        for job in new_jobs:
                            if job['source_job_id'] in job_changes['new_jobs']:
                                job_tuple = (
                                    source['company_id'],
                                    job.get('title', ''),
                                    job.get('location', ''),
                                    job.get('department'),
                                    job.get('description', ''),
                                    job.get('url', ''),
                                    json.dumps(job.get('raw_data', {})),
                                    True,
                                    now,
                                    now,
                                    now,
                                    now,
                                    source['id'],
                                    job['source_job_id'],
                                    job.get('salary_min'),
                                    job.get('salary_max'),
                                    job.get('salary_currency'),
                                    job.get('employment_type'),
                                    job.get('remote_status')
                                )
                                job_tuples.append(job_tuple)
                        
                        if job_tuples:
                            # Create the INSERT statement with ON CONFLICT DO UPDATE
                            fields_str = ', '.join(target_fields)
                            update_fields = [f for f in target_fields if f not in ('company_source_id', 'source_job_id')]
                            update_str = ', '.join([f"{f} = EXCLUDED.{f}" for f in update_fields])
                            
                            # Build query for batch insert
                            placeholders = ', '.join(['%s'] * len(target_fields))
                            values_template = ','.join([f'({placeholders})' for _ in range(len(job_tuples))])
                            
                            # Execute batch insert with ON CONFLICT handling
                            sql = f"""
                                INSERT INTO jobs ({fields_str})
                                VALUES {values_template}
                                ON CONFLICT (company_source_id, source_job_id) 
                                DO UPDATE SET {update_str}
                            """
                            
                            # Flatten parameters and execute
                            params = [item for job_tuple in job_tuples for item in job_tuple]
                            cur.execute(sql, params)
                    
                    # Commit the entire transaction
                    conn.commit()
                    logging.info(f"Successfully updated database for source {source['id']}")
                    
                except Exception as e:
                    conn.rollback()
                    logging.error(f"Error updating database: {str(e)}")
                    raise

    @task
    def update_scrape_time(source: Dict) -> None:
        """
        Updates the next_scrape_time for a company source.
        The scrape_interval in the database is stored in minutes.
        
        Args:
            source: Company source record.
        """
        pg_hook = PostgresHook(postgres_conn_id='postgres_jobs_db')
        now = datetime.utcnow()
        
        # Get the source to check its scrape interval (in minutes)
        sql = """
            SELECT scrape_interval 
            FROM company_sources 
            WHERE id = %(source_id)s
        """
        result = pg_hook.get_first(sql, parameters={'source_id': source['id']})
        interval_minutes = result[0] if result else 1440  # Default to 24 hours (1440 minutes)
        
        # Calculate next scrape time using minutes
        next_scrape = now + timedelta(minutes=interval_minutes)
        
        # Update the source
        sql = """
            UPDATE company_sources 
            SET last_scraped = %(now)s,
                next_scrape_time = %(next_scrape)s
            WHERE id = %(source_id)s
        """
        pg_hook.run(sql, parameters={
            'source_id': source['id'],
            'now': now,
            'next_scrape': next_scrape
        })

    @task
    def handle_source_success(source_and_listings) -> None:
        """Handle source success/failure status based on listings result."""
        source, listings = source_and_listings
        pg_hook = PostgresHook(postgres_conn_id='postgres_jobs_db')
        
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                if listings:
                    # Success case - delete any failure records
                    cur.execute("""
                        DELETE FROM company_source_issues
                        WHERE company_source_id = %(source_id)s
                    """, {'source_id': source['id']})
                else:
                    # Failure case - increment failure count
                    cur.execute("""
                        INSERT INTO company_source_issues (company_source_id, failure_count, last_failure)
                        VALUES (%(source_id)s, 1, NOW())
                        ON CONFLICT (company_source_id) 
                        DO UPDATE SET 
                            failure_count = company_source_issues.failure_count + 1,
                            last_failure = NOW()
                    """, {'source_id': source['id']})
            conn.commit()

    # Get sources to scrape
    sources = get_company_sources_to_scrape()
    
    # Map the scraping task to each source
    listings = scrape_listings.expand(source=sources)
    
    # Process listings for each source-listings pair
    job_changes = process_listings.expand(
        source_and_listings=sources.zip(listings)
    )
    
    # Handle new jobs for each source-changes-listings combination
    detailed_jobs = handle_new_jobs.expand(
        source_changes_and_listings=sources.zip(job_changes, listings)
    )
    
    # Update database for each source-changes-jobs combination
    database_updates = update_database.expand(
        source_changes_and_jobs=sources.zip(job_changes, detailed_jobs)
    )
    
    # Final status updates based on overall processing
    source_status_updates = handle_source_success.expand(
        source_and_listings=sources.zip(detailed_jobs)
    )
    
    # Update scrape times for each source
    scrape_time_updates = update_scrape_time.expand(source=sources)
    
    # Set up task dependencies
    chain(
        listings,
        job_changes,
        detailed_jobs,
        database_updates,
        [source_status_updates, scrape_time_updates]
    )

# Instantiate the DAG
job_scraper_dag()