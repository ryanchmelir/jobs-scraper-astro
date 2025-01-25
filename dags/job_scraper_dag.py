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

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.providers.postgres.hooks.postgres import PostgresHook
from config.settings import SCRAPING_BEE_API_KEY
from infrastructure.models import SourceType
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
    def process_listings(source: Dict, listings: List[Dict]) -> Dict[str, List[str]]:
        """
        Processes scraped listings to identify new, existing, and removed jobs.
        
        Args:
            source: Company source record.
            listings: List of job listings from the scrape (as dictionaries).
            
        Returns:
            Dictionary with lists of job IDs for new, existing, and removed jobs.
        """
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
    def handle_new_jobs(source: Dict, job_changes: Dict[str, List[str]], listings: List[Dict]) -> List[Dict]:
        """
        Scrapes details for new jobs and prepares them for database insertion.
        
        Args:
            source: Company source record.
            job_changes: Dictionary with new, removed, and existing job IDs.
            listings: Original listings data as dictionaries.
            
        Returns:
            List of job listings with full details as dictionaries.
        """
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
                    
                    # Parse job details and merge with listing data
                    job_details = source_handler.parse_job_details(response.text, listing)
                    # Ensure job_details is a dictionary
                    if not isinstance(job_details, dict):
                        job_details = {
                            'description': job_details.description if hasattr(job_details, 'description') else None,
                            'requirements': job_details.requirements if hasattr(job_details, 'requirements') else None,
                            'benefits': job_details.benefits if hasattr(job_details, 'benefits') else None,
                            'additional_fields': job_details.additional_fields if hasattr(job_details, 'additional_fields') else {}
                        }
                    detailed_job = {**listing, **job_details}
                    detailed_jobs.append(detailed_job)
                    
            except Exception as e:
                logging.error(f"Error scraping job details for {listing['id']}: {str(e)}")
                # Continue with other jobs even if one fails
                continue
                
        logging.info(f"Scraped details for {len(detailed_jobs)} new jobs")
        return detailed_jobs

    @task
    def update_database(source: Dict, job_changes: Dict[str, List[str]], listings: List[Dict]) -> None:
        """
        Updates the database with job changes.
        
        Args:
            source: Company source record.
            job_changes: Dictionary with new, removed, and existing job IDs.
            listings: Job listings with full details as dictionaries.
        """
        pg_hook = PostgresHook(postgres_conn_id='postgres_jobs_db')
        now = datetime.utcnow()
        
        # Mark removed jobs as inactive
        if job_changes['removed_jobs']:
            sql = """
                UPDATE jobs 
                SET active = false, last_seen = %(now)s
                WHERE company_source_id = %(source_id)s
                AND source_job_id = ANY(%(job_ids)s)
            """
            pg_hook.run(sql, parameters={
                'source_id': source['id'],
                'job_ids': job_changes['removed_jobs'],
                'now': now
            })
        
        # Update last_seen for existing jobs
        if job_changes['existing_jobs']:
            sql = """
                UPDATE jobs 
                SET last_seen = %(now)s
                WHERE company_source_id = %(source_id)s
                AND source_job_id = ANY(%(job_ids)s)
            """
            pg_hook.run(sql, parameters={
                'source_id': source['id'],
                'job_ids': job_changes['existing_jobs'],
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
                'source_job_id'
            ]
            
            # Convert job dictionaries to tuples matching the target_fields order
            new_jobs = []
            for listing in listings:
                if listing['source_job_id'] in job_changes['new_jobs']:
                    job_tuple = (
                        source['company_id'],        # company_id
                        listing['title'],            # title
                        listing.get('location'),     # location
                        listing.get('department'),   # department
                        listing.get('description'),  # description
                        None,                        # raw_data set to None
                        True,                        # active
                        now,                         # first_seen
                        now,                         # last_seen
                        now,                         # created_at
                        now,                         # updated_at
                        source['id'],                # company_source_id
                        listing['source_job_id']     # source_job_id
                    )
                    new_jobs.append(job_tuple)
            
            if new_jobs:  # Only attempt insert if we have jobs to insert
                pg_hook.insert_rows('jobs', new_jobs, target_fields=target_fields)

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
        source=sources,
        listings=listings
    )
    
    # Handle new jobs for each source-changes-listings combination
    detailed_jobs = handle_new_jobs.expand(
        source=sources,
        job_changes=job_changes,
        listings=listings
    )
    
    # Update database for each source-changes-jobs combination
    database_updates = update_database.expand(
        source=sources,
        job_changes=job_changes,
        listings=detailed_jobs
    )
    
    # Update scrape times for each source
    scrape_time_updates = update_scrape_time.expand(source=sources)
    
    # Define task dependencies
    chain(
        sources,
        listings,
        job_changes,
        detailed_jobs,
        database_updates,
        scrape_time_updates
    )

# Instantiate the DAG
job_scraper_dag()