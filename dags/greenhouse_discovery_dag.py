"""
DAG for discovering new Greenhouse companies via Google Search.
Uses ScrapingBee's Google Search API to find new job boards and creates
company records for ones we don't already track.
"""
from datetime import datetime, timedelta
from typing import List, Dict, Set
import logging
import re
import httpx
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from config.settings import SCRAPING_BEE_API_KEY
from infrastructure.models import SourceType

# Default arguments for all tasks
default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
    'email_on_failure': True,
    'email_on_retry': False,
    'depends_on_past': False,
}

def extract_company_id(url: str) -> str:
    """Extract company ID from Greenhouse URL."""
    # Remove protocol and www if present
    url = url.replace('https://', '').replace('http://', '').replace('www.', '')
    
    # Extract company ID from various URL formats
    patterns = [
        r'boards\.greenhouse\.io/([^/]+)/?',  # matches boards.greenhouse.io/company
        r'job-boards\.greenhouse\.io/([^/]+)/?',  # matches job-boards.greenhouse.io/company
    ]
    
    for pattern in patterns:
        if match := re.search(pattern, url):
            return match.group(1)
    
    return None

@dag(
    dag_id='greenhouse_company_discovery',
    default_args=default_args,
    description='Discovers new Greenhouse companies via Google Search',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['discovery', 'greenhouse'],
)
def greenhouse_discovery_dag():
    """Creates a DAG for discovering new Greenhouse companies."""
    
    @task
    def discover_greenhouse_companies() -> List[Dict]:
        """
        Search Google for Greenhouse job boards using ScrapingBee API.
        Returns list of search results.
        """
        params = {
            'api_key': SCRAPING_BEE_API_KEY,
            'search': 'site:boards.greenhouse.io/ | site:job-boards.greenhouse.io/',
            'language': 'en',
            'extra_params': 'tbs=qdr:d'  # Last 24 hours
        }
        
        try:
            with httpx.Client(timeout=30.0) as client:
                response = client.get(
                    'https://app.scrapingbee.com/api/v1/store/google',
                    params=params
                )
                response.raise_for_status()
                data = response.json()
                
                if not data.get('organic_results'):
                    logging.warning("No search results found")
                    return []
                    
                return data['organic_results']
                
        except Exception as e:
            logging.error(f"Error searching for companies: {str(e)}")
            raise
    
    @task
    def extract_company_ids(search_results: List[Dict]) -> Set[str]:
        """
        Extract unique company IDs from search results.
        Filters out invalid or malformed IDs.
        """
        company_ids = set()
        
        for result in search_results:
            if company_id := extract_company_id(result['url']):
                # Skip common false positives and job detail pages
                if company_id not in ['jobs', 'careers', 'job', 'career'] and '/jobs/' not in result['url']:
                    company_ids.add(company_id)
        
        logging.info(f"Extracted {len(company_ids)} unique company IDs")
        return list(company_ids)
    
    @task
    def filter_existing_companies(company_ids: List[str]) -> List[str]:
        """
        Check which companies don't exist in our database.
        Returns list of new company IDs.
        """
        pg_hook = PostgresHook(postgres_conn_id='postgres_jobs_db')
        
        # Get existing Greenhouse source IDs
        existing_ids = pg_hook.get_records("""
            SELECT source_id 
            FROM company_sources 
            WHERE source_type = %(source_type)s
        """, parameters={'source_type': SourceType.GREENHOUSE.value})
        
        existing_ids = {row[0] for row in existing_ids}
        new_ids = [id for id in company_ids if id not in existing_ids]
        
        logging.info(f"Found {len(new_ids)} new companies")
        return new_ids
    
    @task
    def create_new_companies(new_company_ids: List[str]) -> None:
        """Create new company and company_source records."""
        if not new_company_ids:
            logging.info("No new companies to create")
            return
            
        pg_hook = PostgresHook(postgres_conn_id='postgres_jobs_db')
        now = datetime.utcnow()
        
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                for company_id in new_company_ids:
                    try:
                        # First create the company record
                        cur.execute("""
                            INSERT INTO companies (
                                name,
                                active,
                                created_at,
                                updated_at
                            ) VALUES (
                                %(name)s,
                                true,
                                %(now)s,
                                %(now)s
                            )
                            RETURNING id
                        """, {
                            'name': company_id.replace('-', ' ').title(),
                            'now': now
                        })
                        
                        company_db_id = cur.fetchone()[0]
                        
                        # Then create the company_source record
                        cur.execute("""
                            INSERT INTO company_sources (
                                company_id,
                                source_type,
                                source_id,
                                config,
                                active,
                                last_scraped,
                                next_scrape_time,
                                scrape_interval
                            ) VALUES (
                                %(company_id)s,
                                %(source_type)s,
                                %(source_id)s,
                                %(config)s,
                                true,
                                %(now)s,
                                %(next_scrape)s,
                                %(interval)s
                            )
                        """, {
                            'company_id': company_db_id,
                            'source_type': SourceType.GREENHOUSE.value,
                            'source_id': company_id,
                            'config': {},  # Empty JSON config to start
                            'now': now,
                            'next_scrape': now + timedelta(minutes=1),  # Start scraping in 1 minute
                            'interval': 30  # Default to daily scraping
                        })
                        
                        logging.info(f"Created company and source records for {company_id}")
                        
                    except Exception as e:
                        logging.error(f"Error creating records for {company_id}: {str(e)}")
                        conn.rollback()
                        continue
                        
                conn.commit()
    
    # Set up task dependencies
    search_results = discover_greenhouse_companies()
    company_ids = extract_company_ids(search_results)
    new_companies = filter_existing_companies(company_ids)
    create_new_companies(new_companies)

# Instantiate the DAG
greenhouse_discovery_dag() 