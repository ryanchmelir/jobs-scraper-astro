"""
Greenhouse job board source implementation.
Uses ScrapingBee for fetching pages and lxml for parsing.
"""
from typing import List
from lxml import html
import random

from .base import BaseSource, JobListing

try:
    from config.settings import SCRAPING_BEE_API_KEY
except ImportError:
    SCRAPING_BEE_API_KEY = None  # Will be mocked in tests

class GreenhouseSource(BaseSource):
    """Implementation of BaseSource for Greenhouse job boards."""
    
    def get_listings_url(self, company_source_id: str) -> str:
        """
        Get the URL for a company's Greenhouse job board.
        The company_source_id should be their Greenhouse board name.
        
        Args:
            company_source_id: The Greenhouse board name (e.g. "2ualumni")
            
        Returns:
            Full URL to the job board
        """
        return f"https://boards.greenhouse.io/{company_source_id}"
    
    def parse_listings_page(self, html_content: str) -> List[JobListing]:
        """
        Parse the Greenhouse job board HTML into JobListing objects.
        
        Args:
            html_content: Raw HTML from ScrapingBee
            
        Returns:
            List of JobListing objects with basic info
        """
        listings = []
        tree = html.fromstring(html_content)
        
        # Find all job openings
        for opening in tree.xpath('//div[contains(@class, "opening")]'):
            try:
                # Get job link and title
                job_link = opening.xpath('.//a[@data-mapped]')[0]
                job_url = job_link.get('href')
                title = job_link.text.strip()
                
                # Get location
                location = opening.xpath('.//span[@class="location"]/text()')[0].strip()
                
                # Get departments from the department_id attribute
                departments = opening.get('department_id', '').split(',')
                department = departments[0] if departments else None
                
                # Create listing object
                listing = JobListing(
                    source_job_id=job_url.split('/')[-1],  # Last part of URL is job ID
                    title=title,
                    location=location,
                    department=department,
                    url=f"https://boards.greenhouse.io{job_url}",
                    raw_data={
                        'departments': departments,
                        'office_ids': opening.get('office_id', '').split(','),
                        'html': html.tostring(opening).decode('utf-8')
                    }
                )
                listings.append(listing)
                
            except (IndexError, AttributeError) as e:
                # Log error but continue processing other listings
                print(f"Error parsing job listing: {e}")
                continue
                
        return listings
    
    def get_job_detail_url(self, job_listing: JobListing) -> str:
        """
        Get the URL for a specific job's detail page.
        
        Args:
            job_listing: JobListing object with the job's URL
            
        Returns:
            URL string for the job detail page
        """
        return job_listing.url
    
    def parse_job_details(self, html_content: str, job_listing: JobListing) -> JobListing:
        """Parse the job detail page HTML and update the job listing with full details.
        
        Args:
            html_content: The HTML content of the job detail page
            job_listing: The existing job listing with basic info
            
        Returns:
            Updated JobListing with full details
        """
        tree = html.fromstring(html_content)
        
        # Extract core fields
        title = tree.xpath('//h1[@class="app-title"]/text()')[0].strip()
        location = tree.xpath('//div[@class="location"]/text()')[0].strip()
        
        # Get main content
        content_div = tree.xpath('//div[@id="content"]')[0]
        
        # Try to extract department from content structure
        department = None
        department_elements = content_div.xpath('.//strong[contains(text(), "Department")]')
        if department_elements:
            department = department_elements[0].getnext().text.strip()
        
        # Get full description
        description = html.tostring(content_div, encoding='unicode')
        
        # Create new job listing with updated fields
        return JobListing(
            source_job_id=job_listing.source_job_id,
            title=title,
            location=location,
            department=department,
            raw_data={
                'description': description,
                'detail_html': html_content,
                # Preserve any existing raw data
                **(job_listing.raw_data or {})
            }
        )
    
    def prepare_scraping_config(self, url: str) -> dict:
        """
        Prepare configuration for ScrapingBee API with Greenhouse-specific settings.
        Optimized for raw HTML fetching since Greenhouse serves complete HTML without JS.
        Starting with basic proxy to minimize costs (1 credit/request).
        Can enable premium_proxy later if we see too many failures.
        
        Args:
            url: The URL to be scraped
            
        Returns:
            Dictionary of parameters for ScrapingBee
        """
        config = {
            'api_key': SCRAPING_BEE_API_KEY,
            'url': url,
            'render_js': False,  # Greenhouse serves complete HTML
            'country_code': 'us',  # Use US proxy to avoid region blocks
            'block_resources': True,  # Block images/CSS to speed up
            'timeout': 20000,  # 20 second timeout
            'transparent_status_code': True,  # Get actual status codes
        }
        
        # Add specific wait_for based on URL
        if url.count('/') == 3:  # Main listings page
            config['wait_for'] = '//div[contains(@class, "opening")]'
        else:  # Job detail page
            config['wait_for'] = '//div[@id="content"]'
            
        return config 