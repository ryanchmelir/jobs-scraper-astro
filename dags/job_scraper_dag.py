"""
Job scraper DAG that orchestrates the scraping of job listings from various sources.

This DAG follows a master DAG pattern with dynamic task mapping for scalability.
It handles the selection of company sources to scrape, fetches job listings,
processes them, and updates the database accordingly.
"""

from datetime import datetime, timedelta
from typing import List, Dict, Set
import httpx
import logging

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.utils.helpers import chain
from sqlalchemy import select, update
from sqlalchemy.orm import Session

from config.settings import get_db_engine, SCRAPING_BEE_API_KEY
from infrastructure.models import CompanySource, SourceType, Job
from sources.greenhouse import GreenhouseSource

# Default arguments for all tasks
default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
    'max_active_runs': 1,
}

@dag(
    dag_id='job_scraper',
    default_args=default_args,
    description='Scrapes job listings from configured company sources',
    schedule_interval='@hourly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['scraping', 'jobs'],
)
def job_scraper_dag():
    """Creates a DAG for scraping job listings."""
    
    @task
    def get_company_sources_to_scrape(batch_size: int = 10) -> List[Dict]:
        """
        Selects company sources that are due for scraping.
        
        Args:
            batch_size: Maximum number of sources to process in one run.
            
        Returns:
            List of company source records as dictionaries.
        """
        engine = get_db_engine()
        
        with Session(engine) as session:
            # Query for active sources that are due for scraping
            stmt = select(CompanySource).where(
                CompanySource.active == True,
                (CompanySource.next_scrape_time <= datetime.utcnow()) | 
                (CompanySource.next_scrape_time.is_(None))
            ).limit(batch_size)
            
            sources = session.execute(stmt).scalars().all()
            
            # Convert to dictionaries for XCom serialization
            return [
                {
                    'id': source.id,
                    'company_id': source.company_id,
                    'source_type': source.source_type.value,
                    'source_id': source.source_id,
                    'config': source.config
                }
                for source in sources
            ]

    @task
    def scrape_listings(source: Dict) -> List[Dict]:
        """
        Scrapes job listings for a single company source.
        
        Args:
            source: Company source record as a dictionary.
            
        Returns:
            List of job listings as dictionaries.
        """
        # Initialize the appropriate source handler
        if source['source_type'] == SourceType.GREENHOUSE.value:
            source_handler = GreenhouseSource(source['source_id'])
        else:
            raise ValueError(f"Unsupported source type: {source['source_type']}")
        
        # Get the listings URL and scraping config
        listings_url = source_handler.get_listings_url()
        scraping_config = source_handler.prepare_scraping_config()
        
        # Prepare ScrapingBee request
        params = {
            'api_key': SCRAPING_BEE_API_KEY,
            'url': listings_url,
            'wait': 'domcontentloaded',  # Wait for DOM to be ready
            'premium_proxy': 'true',  # Use premium proxies for better success rate
            **scraping_config  # Add any source-specific config
        }
        
        try:
            # Make the request to ScrapingBee
            logging.info(f"Scraping listings from {listings_url}")
            with httpx.Client(timeout=30.0) as client:
                response = client.get('https://app.scrapingbee.com/api/v1/', params=params)
                response.raise_for_status()
                
                # Parse the listings page
                listings = source_handler.parse_listings_page(response.text)
                logging.info(f"Found {len(listings)} listings")
                return listings
                
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
            source: Company source record as a dictionary.
            listings: List of job listings from the scrape.
            
        Returns:
            Dictionary with lists of job IDs for new, existing, and removed jobs.
        """
        engine = get_db_engine()
        
        with Session(engine) as session:
            # Get existing active jobs for this source
            stmt = select(Job).where(
                Job.company_source_id == source['id'],
                Job.active == True
            )
            existing_jobs = session.execute(stmt).scalars().all()
            
            # Create sets for comparison
            existing_job_ids = {job.source_job_id for job in existing_jobs}
            scraped_job_ids = {listing['id'] for listing in listings}
            
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
            source: Company source record as a dictionary.
            job_changes: Dictionary with new, removed, and existing job IDs.
            listings: Original listings data.
            
        Returns:
            List of job listings with full details.
        """
        if not job_changes['new_jobs']:
            logging.info(f"No new jobs to process for source {source['id']}")
            return []
            
        # Initialize source handler
        if source['source_type'] == SourceType.GREENHOUSE.value:
            source_handler = GreenhouseSource(source['source_id'])
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
                detail_url = source_handler.get_job_detail_url(listing['id'])
                
                # Prepare ScrapingBee request
                params = {
                    'api_key': SCRAPING_BEE_API_KEY,
                    'url': detail_url,
                    'wait': 'domcontentloaded',
                    'premium_proxy': 'true',
                    **source_handler.prepare_scraping_config()
                }
                
                # Make the request
                logging.info(f"Scraping job details from {detail_url}")
                with httpx.Client(timeout=30.0) as client:
                    response = client.get('https://app.scrapingbee.com/api/v1/', params=params)
                    response.raise_for_status()
                    
                    # Parse job details and merge with listing data
                    job_details = source_handler.parse_job_details(response.text)
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
            source: Company source record as a dictionary.
            job_changes: Dictionary with new, removed, and existing job IDs.
            listings: Original listings data for new jobs.
        """
        engine = get_db_engine()
        now = datetime.utcnow()
        
        with Session(engine) as session:
            # Mark removed jobs as inactive
            if job_changes['removed_jobs']:
                stmt = update(Job).where(
                    Job.company_source_id == source['id'],
                    Job.source_job_id.in_(job_changes['removed_jobs'])
                ).values(
                    active=False,
                    last_seen=now
                )
                session.execute(stmt)
            
            # Update last_seen for existing jobs
            if job_changes['existing_jobs']:
                stmt = update(Job).where(
                    Job.company_source_id == source['id'],
                    Job.source_job_id.in_(job_changes['existing_jobs'])
                ).values(
                    last_seen=now
                )
                session.execute(stmt)
            
            # Insert new jobs
            new_jobs = [
                Job(
                    company_id=source['company_id'],
                    company_source_id=source['id'],
                    source_job_id=listing['id'],
                    title=listing['title'],
                    location=listing.get('location'),
                    department=listing.get('department'),
                    raw_data=listing,
                    active=True,
                    first_seen=now,
                    last_seen=now
                )
                for listing in listings
                if listing['id'] in job_changes['new_jobs']
            ]
            if new_jobs:
                session.add_all(new_jobs)
            
            session.commit()

    @task
    def update_scrape_time(source: Dict) -> None:
        """
        Updates the next_scrape_time for a company source.
        
        Args:
            source: Company source record as a dictionary.
        """
        engine = get_db_engine()
        now = datetime.utcnow()
        
        with Session(engine) as session:
            # Get the source to check its scrape interval
            stmt = select(CompanySource).where(
                CompanySource.id == source['id']
            )
            company_source = session.execute(stmt).scalar_one()
            
            # Calculate next scrape time
            interval = timedelta(hours=company_source.scrape_interval or 24)
            next_scrape = now + interval
            
            # Update the source
            company_source.last_scraped = now
            company_source.next_scrape_time = next_scrape
            session.commit()

    # Get sources to scrape
    sources = get_company_sources_to_scrape()
    
    # Map the scraping task to each source
    listings = scrape_listings.expand(source=sources)
    
    # Process listings for each source
    job_changes = process_listings.expand(
        source=sources,
        listings=listings
    )
    
    # Handle new jobs (scrape details)
    detailed_jobs = handle_new_jobs.expand(
        source=sources,
        job_changes=job_changes,
        listings=listings
    )
    
    # Update database for each source
    database_updates = update_database.expand(
        source=sources,
        job_changes=job_changes,
        listings=detailed_jobs  # Use detailed jobs instead of listings
    )
    
    # Update scrape times
    scrape_time_updates = update_scrape_time.expand(
        source=sources
    )
    
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