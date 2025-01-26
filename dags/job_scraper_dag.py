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
from typing import List, Dict
import httpx
import logging
import json

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
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
    'email_on_failure': True,
    'email_on_retry': False,
    'depends_on_past': False,  # Prevent failed runs from blocking next runs
}

@dag(
    dag_id='job_scraper',
    default_args=default_args,
    description='Scrapes job listings from configured company sources',
    schedule_interval='@hourly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['scraping', 'jobs'],
    max_active_tasks=3,  # Limit concurrent tasks
    max_active_runs=1,   # Control concurrent DAG runs
    dagrun_timeout=timedelta(hours=1),  # Entire DAG must complete within 1 hour
)
def job_scraper_dag():
    """Creates a DAG for scraping job listings."""
    
    @task
    def get_company_sources_to_scrape(batch_size: int = 10) -> List[Dict]:
        """
        Selects company sources that are due for scraping.
        
        Args:
            batch_size: Maximum number of sources to scrape in one run.
            
        Returns:
            List of company source records.
        """
        pg_hook = PostgresHook(postgres_conn_id='postgres_jobs_db')
        
        # Query for active sources that are due for scraping
        sql = """
            SELECT 
                id,
                company_id,
                source_type,
                source_id,
                config
            FROM company_sources 
            WHERE active = true 
            AND (next_scrape_time <= NOW() OR next_scrape_time IS NULL)
            LIMIT %(batch_size)s
        """
        sources = pg_hook.get_records(sql, parameters={'batch_size': batch_size})
        
        # Convert to dictionaries
        return [
            {
                'id': source[0],
                'company_id': source[1],
                'source_type': source[2],
                'source_id': source[3],
                'config': source[4]
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
        listings_url = source_handler.get_listings_url(source['source_id'])
        scraping_config = source_handler.prepare_scraping_config(listings_url)
        
        try:
            # Make the request to ScrapingBee
            logging.info(f"Scraping listings from {listings_url}")
            with httpx.Client(timeout=30.0) as client:
                response = client.get('https://app.scrapingbee.com/api/v1/', params=scraping_config)
                response.raise_for_status()
                
                # Parse the listings page and convert to dictionaries
                listings = source_handler.parse_listings_page(response.text)
                listings_dict = [
                    {
                        'id': listing.source_job_id,
                        'source_job_id': listing.source_job_id,  # Store explicitly for URL construction
                        'title': listing.title,
                        'location': listing.location,
                        'department': listing.department,
                        'url': source_handler.get_listing_url(listing),
                        'raw_data': listing.raw_data
                    }
                    for listing in listings
                ]
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
        
        # Scrape details for each new job
        for listing in new_job_listings:
            try:
                # Get job detail URL
                detail_url = source_handler.get_job_detail_url(listing)
                
                # Make the request
                logging.info(f"Scraping job details from {detail_url}")
                with httpx.Client(timeout=30.0) as client:
                    response = client.get('https://app.scrapingbee.com/api/v1/', 
                                       params=source_handler.prepare_scraping_config(detail_url))
                    response.raise_for_status()
                    
                    # Parse job details with better error handling
                    job_details = source_handler.parse_job_details(response.text, listing)
                    if not isinstance(job_details, dict):
                        raise ValueError(f"Expected dictionary from parse_job_details, got {type(job_details)}")
                    
                    # Extract raw_data with validation
                    raw_data = job_details.get('raw_data', {})
                    if not isinstance(raw_data, dict):
                        raw_data = {}
                    
                    # Get metadata with validation
                    metadata = raw_data.get('metadata', {})
                    if not isinstance(metadata, dict):
                        metadata = {}
                    
                    # Get salary range with validation
                    salary_range = raw_data.get('salary_range', {})
                    if not isinstance(salary_range, dict):
                        salary_range = {}
                    
                    # Prepare job record with safe gets
                    detailed_job = {
                        **listing,
                        'description': job_details.get('description', ''),
                        'salary_min': salary_range.get('min'),
                        'salary_max': salary_range.get('max'),
                        'salary_currency': salary_range.get('currency'),
                        'employment_type': raw_data.get('employment_type', 'UNKNOWN'),
                        'remote_status': raw_data.get('remote_status', 'UNKNOWN'),
                        'requirements': raw_data.get('requirements', []),
                        'benefits': raw_data.get('benefits', []),
                        'raw_data': {
                            # Only source-specific data that doesn't fit in structured columns
                            'departments': raw_data.get('departments', []),
                            'office_ids': raw_data.get('office_ids', []),
                            'source': 'greenhouse',
                            'scraped_at': datetime.utcnow().isoformat()
                        },
                        'metadata': {
                            'confidence_scores': metadata.get('confidence_scores', {}),
                            'parser_version': '1.0',
                            'last_parsed': datetime.utcnow().isoformat()
                        }
                    }
                    detailed_jobs.append(detailed_job)
                    
            except Exception as e:
                logging.error(f"Error scraping job details for {listing['id']}: {str(e)}", exc_info=True)
                # Continue with other jobs even if one fails
                continue
                
        logging.info(f"Scraped details for {len(detailed_jobs)} new jobs")
        return detailed_jobs

    @task
    def update_database(source_changes_and_jobs) -> None:
        """
        Updates the database with job changes and their metadata.
        
        Args:
            source_changes_and_jobs: Tuple of (source, job_changes, new_jobs) from upstream tasks.
        """
        source, job_changes, new_jobs = source_changes_and_jobs
        pg_hook = PostgresHook(postgres_conn_id='postgres_jobs_db')
        now = datetime.utcnow()
        
        # Define stale threshold - jobs not seen for this long will be marked inactive
        STALE_THRESHOLD = timedelta(days=7)  # Configurable, adjust as needed
        
        # Mark jobs as inactive if they haven't been seen recently and aren't in current scrape
        sql = """
            UPDATE jobs 
            SET active = false, 
                updated_at = %(now)s  -- Only update the status change timestamp
            WHERE company_source_id = %(source_id)s
            AND active = true  -- Only update currently active jobs
            AND source_job_id != ALL(%(current_job_ids)s)  -- Not in current scrape
            AND (%(now)s - last_seen) > %(stale_threshold)s  -- Haven't been seen recently
        """
        
        current_job_ids = [job['source_job_id'] for job in new_jobs]
        
        result = pg_hook.run(sql, parameters={
            'source_id': source['id'],
            'current_job_ids': current_job_ids,
            'now': now,
            'stale_threshold': STALE_THRESHOLD
        })
        
        if result:
            logging.info(f"Marked jobs as inactive for source {source['id']} that haven't been seen for {STALE_THRESHOLD.days} days")
        
        # Update last_seen for jobs that still exist
        if current_job_ids:
            sql = """
                UPDATE jobs 
                SET last_seen = %(now)s,
                    updated_at = %(now)s
                WHERE company_source_id = %(source_id)s
                AND source_job_id = ANY(%(job_ids)s)
            """
            pg_hook.run(sql, parameters={
                'source_id': source['id'],
                'job_ids': current_job_ids,
                'now': now
            })
        
        # Insert new jobs
        if job_changes['new_jobs']:
            # Define target fields in exact database column order
            target_fields = [
                'company_id',
                'title',
                'location',
                'department',
                'description',
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
                'remote_status',
                'requirements',
                'benefits'
            ]
            
            # Convert job dictionaries to tuples matching the target_fields order
            job_tuples = []
            metadata_tuples = []  # For job_metadata table
            
            for job in new_jobs:
                if job['source_job_id'] in job_changes['new_jobs']:
                    # Extract metadata for separate table
                    metadata = job.get('metadata', {})
                    
                    # Clean raw_data by removing metadata
                    raw_data = job.get('raw_data', {}).copy()
                    raw_data.pop('metadata', None)  # Remove metadata if exists
                    
                    # Convert to JSON strings
                    raw_data_json = json.dumps(raw_data)
                    requirements_json = json.dumps(job.get('requirements', []))
                    benefits_json = json.dumps(job.get('benefits', []))
                    
                    job_tuple = (
                        source['company_id'],                    # company_id
                        job.get('title', ''),                    # title
                        job.get('location', ''),                 # location
                        job.get('department'),                   # department
                        job.get('description', ''),              # description
                        raw_data_json,                           # raw_data (without metadata)
                        True,                                    # active
                        now,                                     # first_seen
                        now,                                     # last_seen
                        now,                                     # created_at
                        now,                                     # updated_at
                        source['id'],                           # company_source_id
                        job['source_job_id'],                   # source_job_id
                        job.get('salary_min'),                  # salary_min
                        job.get('salary_max'),                  # salary_max
                        job.get('salary_currency'),             # salary_currency
                        job.get('employment_type', 'UNKNOWN'),   # employment_type
                        job.get('remote_status', 'UNKNOWN'),     # remote_status
                        requirements_json,                       # requirements
                        benefits_json                           # benefits
                    )
                    job_tuples.append(job_tuple)
                    
                    # Store metadata tuple for later insertion
                    metadata_tuples.append({
                        'source_job_id': job['source_job_id'],
                        'confidence_scores': json.dumps(metadata.get('confidence_scores', {})),
                        'parser_version': metadata.get('parser_version', '1.0'),
                        'last_parsed': metadata.get('last_parsed', now.isoformat()),
                        'parse_count': 1  # First parse
                    })
            
            if job_tuples:  # Only attempt insert if we have jobs to insert
                # Create the INSERT statement with ON CONFLICT DO UPDATE
                fields_str = ', '.join(target_fields)
                placeholders = ', '.join(['%s'] * len(target_fields))
                update_fields = [f for f in target_fields if f not in ('company_source_id', 'source_job_id')]
                update_str = ', '.join([f"{f} = EXCLUDED.{f}" for f in update_fields])
                
                sql = f"""
                    INSERT INTO jobs ({fields_str})
                    VALUES ({placeholders})
                    ON CONFLICT (company_source_id, source_job_id) 
                    DO UPDATE SET {update_str}
                    RETURNING id, source_job_id
                """
                
                # Execute with raw SQL to handle JSON fields properly
                with pg_hook.get_conn() as conn:
                    with conn.cursor() as cur:
                        try:
                            # Insert jobs and get their IDs
                            cur.executemany(sql, job_tuples)
                            job_ids = cur.fetchall()  # Get the returned IDs
                            
                            # Create a mapping of source_job_id to job_id
                            job_id_map = {row[1]: row[0] for row in job_ids}
                            
                            # Insert metadata with job_ids
                            metadata_sql = """
                                INSERT INTO job_metadata 
                                (job_id, confidence_scores, parser_version, last_parsed, parse_count)
                                VALUES (%s, %s, %s, %s, %s)
                                ON CONFLICT (job_id) 
                                DO UPDATE SET
                                    confidence_scores = EXCLUDED.confidence_scores,
                                    parser_version = EXCLUDED.parser_version,
                                    last_parsed = EXCLUDED.last_parsed,
                                    parse_count = job_metadata.parse_count + 1
                            """
                            
                            # Create metadata tuples with job_ids
                            metadata_values = [
                                (
                                    job_id_map[m['source_job_id']],
                                    m['confidence_scores'],
                                    m['parser_version'],
                                    m['last_parsed'],
                                    m['parse_count']
                                )
                                for m in metadata_tuples
                                if m['source_job_id'] in job_id_map
                            ]
                            
                            # Insert metadata
                            cur.executemany(metadata_sql, metadata_values)
                            
                            conn.commit()
                            logging.info(f"Inserted/updated {len(job_tuples)} jobs and their metadata for source {source['id']}")
                        except Exception as e:
                            conn.rollback()
                            logging.error(f"Error inserting jobs and metadata: {str(e)}")
                            raise

    @task
    def update_scrape_time(source: Dict) -> None:
        """
        Updates the next_scrape_time for a company source.
        
        Args:
            source: Company source record.
        """
        pg_hook = PostgresHook(postgres_conn_id='postgres_jobs_db')
        now = datetime.utcnow()
        
        # Get the source to check its scrape interval
        sql = """
            SELECT scrape_interval 
            FROM company_sources 
            WHERE id = %(source_id)s
        """
        result = pg_hook.get_first(sql, parameters={'source_id': source['id']})
        interval_hours = result[0] if result else 24
        
        # Calculate next scrape time
        next_scrape = now + timedelta(hours=interval_hours)
        
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
    
    # Update scrape times for each source
    scrape_time_updates = update_scrape_time.expand(source=sources)
    
    # Set up dependencies between mapped tasks
    # Each mapped instance will maintain its own chain
    chain(
        listings,
        job_changes,
        detailed_jobs,
        database_updates,
        scrape_time_updates
    )

# Instantiate the DAG
job_scraper_dag()