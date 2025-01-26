"""
Greenhouse job board source implementation.
Uses ScrapingBee for fetching pages and lxml for parsing.
"""
from typing import List, Dict
from lxml import html
import re
from datetime import datetime
import logging
import html2text
import time
from threading import Lock

from .base import BaseSource, JobListing

try:
    from config.settings import SCRAPING_BEE_API_KEY
except ImportError:
    SCRAPING_BEE_API_KEY = None  # Will be mocked in tests

class RateLimiter:
    """Simple rate limiter for API calls."""
    def __init__(self, calls: int, period: float):
        self.calls = calls  # Number of calls allowed
        self.period = period  # Time period in seconds
        self.timestamps = []  # Timestamp of each call
        self.lock = Lock()  # Thread safety

    def acquire(self):
        """Wait if necessary and record the timestamp."""
        with self.lock:
            now = time.time()
            # Remove timestamps older than our period
            self.timestamps = [ts for ts in self.timestamps if ts > now - self.period]
            
            # If we've hit our limit, wait
            if len(self.timestamps) >= self.calls:
                sleep_time = self.timestamps[0] - (now - self.period)
                if sleep_time > 0:
                    time.sleep(sleep_time)
                self.timestamps = self.timestamps[1:]
            
            # Record this call
            self.timestamps.append(now)

class GreenhouseSource(BaseSource):
    """Implementation of BaseSource for Greenhouse job boards."""
    
    # Class-level rate limiter: 10 requests per minute
    _rate_limiter = RateLimiter(calls=10, period=60.0)
    
    def __init__(self):
        super().__init__()
        self.html_converter = html2text.HTML2Text()
        self.html_converter.ignore_links = False
        self.html_converter.ignore_images = True
        self.html_converter.body_width = 0  # Don't wrap text
        self.html_converter.protect_links = True  # Don't wrap links
        self.html_converter.unicode_snob = True  # Use Unicode
        self.html_converter.ul_item_mark = '-'  # Use - for unordered lists
    
    # Employment and remote work patterns
    EMPLOYMENT_PATTERNS = [
        r'(full[- ]time|part[- ]time)',
        r'(permanent|contract|temporary)',
        r'(internship|co-op)',
        r'(\d+|full)[- ]time equivalent'
    ]

    REMOTE_PATTERNS = [
        r'(remote|hybrid|office[- ]first)',
        r'(in[- ]office|on[- ]site)',
        r'(work[- ]from[- ]home|wfh)',
        r'(\d+\s*days?\s*(in|remote))'
    ]

    SALARY_PATTERNS = [
        r'\$?(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)[k+]?(?:\s*[-–]\s*\$?(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)[k+]?)?',
        r'~\s*\$?(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)[k+]?',
        r'Salary:\s*\$?(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)[k+]?',
        r'(\d{2,3})[k+](?:\s*-\s*(\d{2,3})[k+])?',
        r'(?:USD|EUR|GBP)?\s*\$?(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)',
        r'compensation.*?\$(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)',
        r'range.*?\$(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)'
    ]

    LOCATION_PATTERNS = [
        r'((?:remote|hybrid)\s+in\s+[^\.;]+)',
        r'((?:based|located)\s+in\s+[^\.;]+)',
        r'(location:\s*[^\.;]+)',
        r'([A-Z][a-zA-Z\s]+,\s+[A-Z]{2})',  # City, State
        r'([A-Z][a-zA-Z\s]+,\s+[A-Z][a-zA-Z\s]+)'  # City, Country
    ]

    def get_listings_url(self, company_source_id: str) -> str:
        """Get the URL for a company's Greenhouse job board."""
        return f"https://boards.greenhouse.io/{company_source_id}"
    
    def _make_request(self, url: str) -> str:
        """Make a rate-limited request to ScrapingBee."""
        import httpx
        
        # Acquire rate limit token
        self._rate_limiter.acquire()
        
        # Make the request
        with httpx.Client(timeout=30.0) as client:
            response = client.get('https://app.scrapingbee.com/api/v1/',
                              params=self.prepare_scraping_config(url))
            response.raise_for_status()
            return response.text

    def parse_listings_page(self, html_content: str) -> List[JobListing]:
        """Parse the Greenhouse job board HTML into JobListing objects."""
        listings = []
        tree = html.fromstring(html_content)
        
        for opening in tree.xpath('//div[contains(@class, "opening")]'):
            try:
                job_link = opening.xpath('.//a[@data-mapped]')[0]
                job_url = job_link.get('href')
                title = job_link.text.strip()
                location = opening.xpath('.//span[@class="location"]/text()')[0].strip()
                departments = opening.get('department_id', '').split(',')
                department = departments[0] if departments else None
                
                listing = JobListing(
                    source_job_id=job_url.split('/')[-1],
                    title=title,
                    location=location,
                    department=department,
                    url=f"https://boards.greenhouse.io{job_url}",
                    raw_data={
                        'departments': departments,
                        'office_ids': opening.get('office_id', '').split(','),
                        'source': 'greenhouse',
                        'scraped_at': datetime.utcnow().isoformat()
                    }
                )
                listings.append(listing)
                
            except (IndexError, AttributeError) as e:
                logging.error(f"Error parsing job listing: {e}")
                continue
                
        return listings
    
    def get_listing_url(self, listing) -> str:
        """Get the URL for a job listing."""
        if isinstance(listing, (JobListing, dict)):
            try:
                return listing.url if isinstance(listing, JobListing) else listing['url']
            except (AttributeError, KeyError):
                raise ValueError("Listing must have a 'url' field")
        raise TypeError(f"Expected JobListing or dict, got {type(listing)}")
        
    def get_job_detail_url(self, listing) -> str:
        """Get the URL for a specific job's detail page."""
        return self.get_listing_url(listing)

    def _extract_with_patterns(self, text: str, patterns: List[str]) -> str:
        """Helper to extract text using a list of patterns."""
        for pattern in patterns:
            match = re.search(pattern, text, re.I)
            if match:
                return match.group(1)
        return ""

    def _get_element_text(self, element) -> str:
        """Safely get text content from an element."""
        if element is None:
            return ""
        if isinstance(element, str):
            return element.strip()
        return element.text_content().strip()

    def _clean_html_content(self, content_div) -> str:
        """Clean HTML content and convert to markdown."""
        if content_div is None:
            return ""
            
        # Remove unwanted elements
        for elem in content_div.xpath('.//script | .//style | .//form | .//button | .//iframe'):
            elem.getparent().remove(elem)
            
        # Convert div to string while preserving structure
        html_str = html.tostring(content_div, encoding='unicode')
        
        # Convert to markdown
        return self.html_converter.handle(html_str).strip()

    def parse_job_details(self, html_content: str, job_listing: dict | JobListing) -> dict:
        """Parse job details from HTML content."""
        tree = html.fromstring(html_content)
        
        try:
            # Try different title selectors in order of preference
            title_elems = (
                tree.xpath('//h1[contains(@class, "section-header")]') or
                tree.xpath('//h1[@class="app-title"]') or
                tree.xpath('//h1[contains(@class, "job-title")]') or
                tree.xpath('//h1[contains(@class, "position-title")]') or
                tree.xpath('//meta[@property="og:title"]/@content') or
                tree.xpath('//title/text()')
            )
            title = self._get_element_text(title_elems[0]) if title_elems else ""
            
            # Try different location selectors
            location_elems = (
                tree.xpath('//div[contains(@class, "job__location")]//text()') or
                tree.xpath('//div[@class="location"]/text()') or
                tree.xpath('//span[contains(@class, "location")]/text()') or
                tree.xpath('//meta[@property="og:description"]/@content') or
                tree.xpath('//*[contains(text(), "Location:")]/following-sibling::*[1]/text()')
            )
            location = ' '.join(t.strip() for t in location_elems if t.strip())[:255] if location_elems else ""
            
            # Try different content selectors
            content_elems = (
                tree.xpath('//div[contains(@class, "job__description")]') or
                tree.xpath('//div[@id="content"]') or
                tree.xpath('//article[contains(@class, "job-post")]') or
                tree.xpath('//main[contains(@class, "job-post")]') or
                tree.xpath('//div[contains(@class, "description")]')
            )
            
            description_text = ""
            employment_type = None
            remote_status = None
            salary = None
            
            if content_elems:
                content_div = content_elems[0]
                
                # Convert content to markdown
                description_text = self._clean_html_content(content_div)
                
                # Extract patterns from the markdown text
                employment_type = self._extract_with_patterns(description_text, self.EMPLOYMENT_PATTERNS)
                remote_status = self._extract_with_patterns(description_text, self.REMOTE_PATTERNS)
                
                # If location is empty, try to find it in the text
                if not location:
                    location = self._extract_with_patterns(description_text, self.LOCATION_PATTERNS)
            
            # Try to find salary information in multiple places
            salary_elems = (
                tree.xpath('//div[contains(@class, "job__pay-ranges")]') or
                tree.xpath('//div[contains(@class, "pay-range")]') or
                tree.xpath('//div[contains(@class, "compensation")]') or
                tree.xpath('//p[contains(translate(text(), "SALARY", "salary"), "salary")]') or
                tree.xpath('//li[contains(translate(text(), "SALARY", "salary"), "salary")]')
            )
            
            if salary_elems:
                salary_text = self._get_element_text(salary_elems[0])
                for pattern in self.SALARY_PATTERNS:
                    match = re.search(pattern, salary_text)
                    if match:
                        salary = match.group(0)
                        break
            
            # Get source_job_id from input
            source_job_id = job_listing.source_job_id if isinstance(job_listing, JobListing) else job_listing['source_job_id']
            source_job_id = str(source_job_id)[:255]
            
            # Get existing raw data while preserving non-HTML fields
            existing_raw_data = job_listing.raw_data if isinstance(job_listing, JobListing) else job_listing.get('raw_data', {})
            raw_data = {k: v for k, v in existing_raw_data.items() if k not in ('html', 'detail_html')}
            
            # Update raw_data with structured information
            raw_data.update({
                'departments': raw_data.get('departments', []),
                'office_ids': raw_data.get('office_ids', []),
                'employment_type': employment_type,
                'remote_status': remote_status,
                'salary': salary,
                'metadata': {
                    'scraped_at': datetime.utcnow().isoformat(),
                    'source': 'greenhouse'
                }
            })
            
            # Get current timestamp for temporal fields
            now = datetime.utcnow()
            
            return {
                'source_job_id': source_job_id,
                'title': title,
                'location': location,
                'department': None,  # Will be set by DAG
                'description': description_text,
                'url': self.get_listing_url(job_listing),  # Add URL as main field
                'raw_data': raw_data,
                'active': True,
                'first_seen': now,
                'last_seen': now,
                'created_at': now,
                'updated_at': now
            }
            
        except Exception as e:
            logging.error(f"Error parsing job details: {e}")
            return {
                'source_job_id': job_listing.source_job_id if isinstance(job_listing, JobListing) else job_listing['source_job_id'],
                'title': '',
                'location': '',
                'department': None,
                'description': '',
                'url': '',
                'raw_data': {'error': str(e)},
                'active': False
            }
    
    def prepare_scraping_config(self, url: str) -> dict:
        """Prepare configuration for ScrapingBee API with Greenhouse-specific settings."""
        config = super().prepare_scraping_config(url)
        config['api_key'] = SCRAPING_BEE_API_KEY
        
        if url.count('/') == 3:  # Main listings page
            config['wait_for'] = '//div[contains(@class, "opening")]'
        else:  # Job detail page
            config['wait_for'] = '//div[@id="content"]'
            
        return config 