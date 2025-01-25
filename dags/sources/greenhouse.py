"""
Greenhouse job board source implementation.
Uses ScrapingBee for fetching pages and lxml for parsing.
"""
from typing import List, Optional, Tuple
from lxml import html
import logging
from datetime import datetime

from .base import BaseSource, JobListing

try:
    from config.settings import SCRAPING_BEE_API_KEY
except ImportError:
    SCRAPING_BEE_API_KEY = None  # Will be mocked in tests

class GreenhouseSource(BaseSource):
    """Implementation of BaseSource for Greenhouse job boards."""
    
    # Selectors for different Greenhouse implementations
    LISTING_SELECTORS = [
        ('standard', '//div[contains(@class, "opening")]'),
        ('mapped', '//a[@data-mapped="true"]/..'),
        ('table', '//tr[contains(@class, "TableRow")]')
    ]
    
    LOCATION_SELECTORS = [
        './/span[@class="location"]/text()',
        './/td[contains(@class, "location")]/text()',
        './/div[contains(@class, "location")]/text()'
    ]
    
    TITLE_SELECTORS = [
        './/a[@data-mapped]/text()',
        './/a[contains(@href, "/jobs/")]/text()',
        './/td[contains(@class, "title")]//a/text()'
    ]
    
    # Add new class constants for job detail selectors
    TITLE_DETAIL_SELECTORS = [
        '//h1[@class="app-title"]/text()',
        '//h1[contains(@class, "job-title")]/text()',
        '//h1[contains(@class, "posting-headline")]/text()'
    ]
    
    LOCATION_DETAIL_SELECTORS = [
        '//div[@class="location"]/text()',
        '//div[contains(@class, "job-location")]/text()',
        '//span[contains(@class, "location")]/text()'
    ]
    
    DEPARTMENT_DETAIL_SELECTORS = [
        './/strong[contains(text(), "Department")]/following-sibling::text()',
        './/div[contains(@class, "department")]/text()',
        './/span[contains(@class, "department")]/text()'
    ]
    
    CONTENT_DETAIL_SELECTORS = [
        '//div[@id="content"]',
        '//div[contains(@class, "job-content")]',
        '//div[contains(@class, "posting-content")]'
    ]
    
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
    
    def _extract_text(self, element, selectors: List[str]) -> Optional[str]:
        """
        Try multiple selectors to extract text from an element.
        
        Args:
            element: The lxml element to search
            selectors: List of xpath selectors to try
            
        Returns:
            Extracted text if found, None otherwise
        """
        for selector in selectors:
            try:
                result = element.xpath(selector)
                if result:
                    return result[0].strip()
            except (IndexError, AttributeError) as e:
                continue
        return None
    
    def _extract_job_data(self, element, implementation: str) -> Optional[Tuple[str, str, str, str, dict]]:
        """
        Extract job data from an element using appropriate selectors.
        
        Args:
            element: The lxml element containing job data
            implementation: Which Greenhouse implementation was detected
            
        Returns:
            Tuple of (job_id, title, location, department, raw_data) or None if extraction fails
        """
        try:
            # Get job link and ID
            job_link = element.xpath('.//a[@data-mapped]')[0]
            job_url = job_link.get('href')
            job_id = job_url.split('/')[-1]
            
            # Get title and location
            title = self._extract_text(element, self.TITLE_SELECTORS)
            location = self._extract_text(element, self.LOCATION_SELECTORS)
            
            if not all([job_id, title, location]):
                return None
                
            # Get departments (consistent across implementations)
            departments = element.get('department_id', '').split(',')
            department = departments[0] if departments else None
            
            # Collect raw data
            raw_data = {
                'departments': departments,
                'office_ids': element.get('office_id', '').split(','),
                'implementation': implementation
            }
            
            return job_id, title, location, department, raw_data
            
        except (IndexError, AttributeError) as e:
            logging.debug(f"Failed to extract job data: {e}")
            return None
    
    def parse_listings_page(self, html_content: str) -> List[JobListing]:
        """
        Parse the Greenhouse job board HTML into JobListing objects.
        Tries multiple selectors to handle different Greenhouse implementations.
        
        Args:
            html_content: Raw HTML from ScrapingBee
            
        Returns:
            List of JobListing objects with basic info
        """
        listings = []
        tree = html.fromstring(html_content)
        
        # Try each implementation's selectors
        for impl_name, selector in self.LISTING_SELECTORS:
            elements = tree.xpath(selector)
            if not elements:
                continue
                
            logging.info(f"Found {len(elements)} listings using {impl_name} implementation")
            
            for element in elements:
                try:
                    job_data = self._extract_job_data(element, impl_name)
                    if not job_data:
                        continue
                        
                    job_id, title, location, department, raw_data = job_data
                    
                    listing = JobListing(
                        source_job_id=job_id,
                        title=title,
                        location=location,
                        department=department,
                        url=f"https://boards.greenhouse.io{job_url}",
                        raw_data=raw_data
                    )
                    listings.append(listing)
                    
                except Exception as e:
                    logging.warning(f"Error parsing job listing: {e}")
                    continue
            
            # If we found listings with this implementation, stop trying others
            if listings:
                break
                
        return listings
    
    def get_listing_url(self, listing) -> str:
        """
        Get the URL for a job listing.
        
        Args:
            listing: Either a JobListing object or a dictionary with job data
            
        Returns:
            Full URL for the job listing
        """
        # If it's a JobListing object or dictionary, get the URL
        if isinstance(listing, (JobListing, dict)):
            try:
                return listing.url if isinstance(listing, JobListing) else listing['url']
            except (AttributeError, KeyError):
                raise ValueError("Listing must have a 'url' field")
            
        raise TypeError(f"Expected JobListing or dict, got {type(listing)}")
        
    def get_job_detail_url(self, listing) -> str:
        """
        Get the URL for a specific job's detail page.
        
        Args:
            listing: Either a JobListing object or a dictionary with the job's data
            
        Returns:
            URL string for the job detail page
        """
        return self.get_listing_url(listing)
    
    def parse_job_details(self, html_content: str, job_listing: dict | JobListing) -> dict:
        """Parse the job detail page HTML and update the job listing with full details.
        
        Args:
            html_content: The HTML content of the job detail page
            job_listing: The existing job listing with basic info (dict or JobListing)
            
        Returns:
            Dictionary matching Job model schema with parsed details
        """
        tree = html.fromstring(html_content)
        
        # Extract core fields using multiple selectors
        title = None
        for selector in self.TITLE_DETAIL_SELECTORS:
            try:
                elements = tree.xpath(selector)
                if elements:
                    title = elements[0].strip()[:255]
                    break
            except (IndexError, AttributeError) as e:
                continue
                
        if not title:
            # Fallback to listing title if available
            title = (job_listing.title if isinstance(job_listing, JobListing) 
                    else job_listing.get('title', 'Unknown Position'))[:255]
            logging.warning(f"Failed to extract title from detail page, using listing title: {title}")
            
        # Extract location with fallbacks
        location = None
        for selector in self.LOCATION_DETAIL_SELECTORS:
            try:
                elements = tree.xpath(selector)
                if elements:
                    location = elements[0].strip()[:255]
                    break
            except (IndexError, AttributeError) as e:
                continue
                
        if not location:
            # Fallback to listing location
            location = (job_listing.location if isinstance(job_listing, JobListing)
                      else job_listing.get('location', 'Unknown Location'))[:255]
            logging.warning(f"Failed to extract location from detail page, using listing location: {location}")
        
        # Try to extract department using multiple selectors
        department = None
        for selector in self.DEPARTMENT_DETAIL_SELECTORS:
            try:
                elements = tree.xpath(selector)
                if elements:
                    department = elements[0].strip()[:255]
                    break
            except (IndexError, AttributeError) as e:
                continue
        
        # Extract content with fallbacks
        description_text = []
        content_found = False
        
        for selector in self.CONTENT_DETAIL_SELECTORS:
            try:
                content_elements = tree.xpath(selector)
                if content_elements:
                    content_div = content_elements[0]
                    # Get all text nodes while preserving structure
                    for element in content_div.xpath('.//text()'):
                        text = element.strip()
                        if text and not text.startswith('Apply'):  # Skip application buttons
                            description_text.append(text)
                    content_found = True
                    break
            except (IndexError, AttributeError) as e:
                continue
                
        if not content_found:
            logging.warning("Failed to extract job description content")
            description_text = ["No description available"]
        
        # Get source_job_id from input
        source_job_id = job_listing.source_job_id if isinstance(job_listing, JobListing) else job_listing['source_job_id']
        source_job_id = str(source_job_id)[:255]  # Ensure string and length
        
        # Get existing raw data while preserving non-HTML fields
        existing_raw_data = job_listing.raw_data if isinstance(job_listing, JobListing) else job_listing.get('raw_data', {})
        raw_data = {
            k: v for k, v in existing_raw_data.items() 
            if k not in ('html', 'detail_html')  # Exclude HTML fields
        }
        
        # Add any additional structured data to raw_data
        raw_data.update({
            'departments': raw_data.get('departments', []),
            'office_ids': raw_data.get('office_ids', []),
            'metadata': {
                'scraped_at': datetime.utcnow().isoformat(),
                'source': 'greenhouse',
                'detail_selectors_used': {
                    'title': next((s for s in self.TITLE_DETAIL_SELECTORS if tree.xpath(s)), None),
                    'location': next((s for s in self.LOCATION_DETAIL_SELECTORS if tree.xpath(s)), None),
                    'department': next((s for s in self.DEPARTMENT_DETAIL_SELECTORS if tree.xpath(s)), None),
                    'content': next((s for s in self.CONTENT_DETAIL_SELECTORS if tree.xpath(s)), None)
                }
            }
        })
        
        # Get current timestamp for temporal fields
        now = datetime.utcnow()
        
        # Return dictionary matching Job model schema exactly
        return {
            # Required fields
            'source_job_id': source_job_id,
            'title': title,
            'location': location,
            'department': department,
            'description': '\n'.join(description_text),
            'raw_data': raw_data,
            
            # Default fields
            'active': True,
            'first_seen': now,
            'last_seen': now,
            'created_at': now,
            'updated_at': now
        }
    
    def prepare_scraping_config(self, url: str) -> dict:
        """
        Prepare configuration for ScrapingBee API with Greenhouse-specific settings.
        Inherits base configuration optimized for minimal API credit usage.
        Adds Greenhouse-specific wait_for selectors based on URL type and implementation.
        
        Args:
            url: The URL to be scraped
            
        Returns:
            Dictionary of parameters for ScrapingBee
        """
        # Get base configuration
        config = super().prepare_scraping_config(url)
        
        # Add API key
        config['api_key'] = SCRAPING_BEE_API_KEY
        
        # Add specific wait_for based on URL type
        if url.count('/') == 3:  # Main listings page
            # Wait for any of our known listing implementations
            config['wait_for'] = ' | '.join(selector for _, selector in self.LISTING_SELECTORS)
        else:  # Job detail page
            # Wait for any of our known content selectors
            config['wait_for'] = ' | '.join(self.CONTENT_DETAIL_SELECTORS)
            
        # Adjust timeout based on page type
        config['timeout'] = 30000 if url.count('/') > 3 else 20000  # Longer timeout for detail pages
            
        return config 