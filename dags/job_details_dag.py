"""
Job details DAG that handles deep scraping of job listings.

This DAG focuses on scraping detailed information for newly discovered jobs.
It runs less frequently than the discovery DAG and handles all the structured
data extraction and parsing.

Scheduling Information:
- Runs every hour
- Does not perform catchup for missed intervals
- High SQL concurrency for batch processing
- Uses 'scraping_bee' pool to limit to 5 concurrent requests across all DAGs
- Longer timeout per task for thorough processing
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
from scraping.parsers import (
    extract_structured_salary,
    extract_structured_location,
    extract_skills,
    extract_seniority,
    normalize_employment_type,
    normalize_remote_status
)

# Default arguments for all tasks
default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=10),
    'email_on_failure': True,
    'email_on_retry': False,
    'depends_on_past': False,
}

@dag(
    dag_id='job_details',
    default_args=default_args,
    description='Scrapes detailed information for newly discovered jobs',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['scraping', 'jobs', 'details'],
    max_active_tasks=20,  # High SQL concurrency
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
)
def job_details_dag():
    """Creates a DAG for scraping detailed job information."""
    
    @task
    def get_jobs_needing_details(batch_size: int = 100) -> List[Dict]:
        """
        Gets jobs that need detailed information scraped.
        
        Args:
            batch_size: Maximum number of jobs to process in one run.
            
        Returns:
            List of job records that need details.
        """
        pg_hook = PostgresHook(postgres_conn_id='postgres_jobs_db')
        
        sql = """
            SELECT 
                j.id as job_id,
                j.company_id,
                j.source_job_id,
                j.url,
                cs.id as company_source_id,
                cs.source_type,
                cs.source_id,
                cs.config
            FROM jobs j
            JOIN company_sources cs ON j.company_source_id = cs.id
            WHERE j.needs_details = true
            AND j.active = true
            AND NOT EXISTS (
                -- Skip jobs that have failed too many times recently
                SELECT 1 FROM job_scraping_issues jsi
                WHERE jsi.job_id = j.id
                AND jsi.failure_count >= 3
                AND jsi.last_failure > NOW() - INTERVAL '24 hours'
            )
            ORDER BY j.created_at ASC
            LIMIT %(batch_size)s
            FOR UPDATE SKIP LOCKED
        """
        jobs = pg_hook.get_records(sql, parameters={'batch_size': batch_size})
        
        # Split jobs into chunks of 5 to respect ScrapingBee's concurrent request limit
        chunked_jobs = []
        for i in range(0, len(jobs), 5):
            chunk = jobs[i:i+5]
            chunked_jobs.extend([
                {
                    'job_id': job[0],
                    'company_id': job[1],
                    'source_job_id': job[2],
                    'url': job[3],
                    'company_source_id': job[4],
                    'source_type': job[5],
                    'source_id': job[6],
                    'config': job[7],
                    'chunk_id': i // 5  # Add chunk ID for potential use in rate limiting
                }
                for job in chunk
            ])
        
        return chunked_jobs

    @task(pool='scraping_bee', pool_slots=1)
    def scrape_job_details(job: Dict) -> Dict:
        """
        Scrapes detailed information for a single job.
        Uses Airflow pool to ensure max 5 concurrent requests to ScrapingBee across all DAGs.
        Properly handles rate limiting and updates job_scraping_issues on failures.
        
        Args:
            job: Job record to scrape details for.
            
        Returns:
            Dictionary with job details and structured data.
            
        Raises:
            AirflowException: On rate limiting (429) to trigger retry
            ValueError: On unsupported source types
        """
        if job['source_type'] == SourceType.GREENHOUSE.value:
            source_handler = GreenhouseSource()
        else:
            raise ValueError(f"Unsupported source type: {job['source_type']}")
        
        pg_hook = PostgresHook(postgres_conn_id='postgres_jobs_db')
        
        try:
            # Get job detail URL and scraping config
            detail_url = source_handler.get_job_detail_url({
                'source_job_id': job['source_job_id'],
                'url': job['url'],
                'raw_data': {'source_id': job['source_id']}
            }, job.get('config', {}))
            
            scraping_config = source_handler.prepare_scraping_config(detail_url)
            
            logging.info(f"Scraping details from {detail_url} (chunk {job['chunk_id']})")
            with httpx.Client(timeout=30.0) as client:
                response = client.get('https://app.scrapingbee.com/api/v1/', params=scraping_config)
                
                # Handle rate limiting explicitly
                if response.status_code == 429:
                    error_msg = "Rate limited by ScrapingBee (429)"
                    logging.warning(f"{error_msg} for job {job['job_id']}")
                    
                    # Update job_scraping_issues
                    pg_hook.run("""
                        INSERT INTO job_scraping_issues (job_id, failure_count, last_failure, last_error)
                        VALUES (%(job_id)s, 1, NOW(), %(error)s)
                        ON CONFLICT (job_id) DO UPDATE SET
                            failure_count = job_scraping_issues.failure_count + 1,
                            last_failure = NOW(),
                            last_error = %(error)s
                    """, parameters={
                        'job_id': job['job_id'],
                        'error': error_msg
                    })
                    
                    from airflow.exceptions import AirflowException
                    raise AirflowException(error_msg)
                
                response.raise_for_status()
                
                # Parse job details
                details = source_handler.parse_job_details(response.text, {
                    'source_job_id': job['source_job_id'],
                    'url': job['url'],
                    'raw_data': {}
                })
                
                # Add structured data
                description = details.get('description', '')
                title = details.get('title', '')
                location = details.get('location', '')
                
                structured_data = {
                    'salary': extract_structured_salary(description),
                    'location': extract_structured_location(location),
                    'skills': extract_skills(description),
                    'seniority': extract_seniority(title + ' ' + description),
                    'employment_type': normalize_employment_type(
                        details.get('raw_data', {}).get('employment_type')
                    ),
                    'remote_status': normalize_remote_status(
                        details.get('raw_data', {}).get('remote_status') or location
                    )
                }
                
                if details.get('raw_data'):
                    details['raw_data']['structured_data'] = structured_data
                else:
                    details['raw_data'] = {'structured_data': structured_data}
                
                # Clear any previous issues on success
                pg_hook.run("""
                    DELETE FROM job_scraping_issues
                    WHERE job_id = %(job_id)s
                """, parameters={'job_id': job['job_id']})
                
                return {
                    'job_id': job['job_id'],
                    'details': details,
                    'success': True
                }
                
        except Exception as e:
            error_msg = str(e)
            logging.error(f"Error scraping job {job['job_id']}: {error_msg}")
            
            # Don't update issues table for rate limiting as it's handled above
            if not isinstance(e, AirflowException):
                pg_hook.run("""
                    INSERT INTO job_scraping_issues (job_id, failure_count, last_failure, last_error)
                    VALUES (%(job_id)s, 1, NOW(), %(error)s)
                    ON CONFLICT (job_id) DO UPDATE SET
                        failure_count = job_scraping_issues.failure_count + 1,
                        last_failure = NOW(),
                        last_error = %(error)s
                """, parameters={
                    'job_id': job['job_id'],
                    'error': error_msg
                })
            
            raise  # Re-raise to trigger task failure

    @task
    def save_job_details(result: Dict) -> None:
        """
        Saves scraped job details to the database.
        
        Args:
            result: Result from scraping job details.
        """
        pg_hook = PostgresHook(postgres_conn_id='postgres_jobs_db')
        now = datetime.utcnow()
        
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                try:
                    if result['success']:
                        details = result['details']
                        structured_data = details.get('raw_data', {}).get('structured_data', {})
                        
                        # Update job with details
                        cur.execute("""
                            UPDATE jobs
                            SET title = %(title)s,
                                location = %(location)s,
                                department = %(department)s,
                                description = %(description)s,
                                url = %(url)s,
                                raw_data = %(raw_data)s,
                                salary_min = %(salary_min)s,
                                salary_max = %(salary_max)s,
                                salary_currency = %(salary_currency)s,
                                employment_type = %(employment_type)s,
                                remote_status = %(remote_status)s,
                                needs_details = false,
                                updated_at = %(now)s
                            WHERE id = %(job_id)s
                        """, {
                            'job_id': result['job_id'],
                            'title': details.get('title'),
                            'location': details.get('location'),
                            'department': details.get('department'),
                            'description': details.get('description'),
                            'url': details.get('url'),
                            'raw_data': json.dumps(details.get('raw_data', {})),
                            'salary_min': structured_data.get('salary', {}).get('amount_min'),
                            'salary_max': structured_data.get('salary', {}).get('amount_max'),
                            'salary_currency': structured_data.get('salary', {}).get('currency'),
                            'employment_type': structured_data.get('employment_type'),
                            'remote_status': structured_data.get('remote_status'),
                            'now': now
                        })
                        
                        # Clear any scraping issues
                        cur.execute("""
                            DELETE FROM job_scraping_issues
                            WHERE job_id = %(job_id)s
                        """, {'job_id': result['job_id']})
                        
                    else:
                        # Record scraping failure
                        cur.execute("""
                            INSERT INTO job_scraping_issues (
                                job_id,
                                failure_count,
                                last_failure,
                                last_error
                            ) VALUES (
                                %(job_id)s,
                                1,
                                %(now)s,
                                %(error)s
                            )
                            ON CONFLICT (job_id) DO UPDATE
                            SET failure_count = job_scraping_issues.failure_count + 1,
                                last_failure = EXCLUDED.last_failure,
                                last_error = EXCLUDED.last_error
                        """, {
                            'job_id': result['job_id'],
                            'now': now,
                            'error': result.get('error', 'Unknown error')
                        })
                    
                    conn.commit()
                    
                except Exception as e:
                    conn.rollback()
                    logging.error(f"Error saving job details: {str(e)}")
                    raise

    # Get jobs that need details
    jobs = get_jobs_needing_details()
    
    # Map detail scraping to each job
    details = scrape_job_details.expand(job=jobs)
    
    # Save the details
    save_results = save_job_details.expand(result=details)
    
    # Set up dependencies
    chain(
        jobs,
        details,
        save_results
    )

# Instantiate the DAG
job_details_dag() 