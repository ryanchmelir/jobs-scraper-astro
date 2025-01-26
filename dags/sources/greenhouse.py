"""
Greenhouse job board source implementation.
Uses ScrapingBee for fetching pages and lxml for parsing.
"""
from typing import List, Dict, Tuple, Optional
from lxml import html
import re
from datetime import datetime
import logging
import html2text
import time
from threading import Lock
import httpx

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
    
    # URL patterns for Greenhouse job boards, ordered by preference
    BASE_URLS = [
        "https://job-boards.greenhouse.io/embed/job_board?for={company}",  # Most reliable
        "https://boards.greenhouse.io/embed/job_board?for={company}",
        "https://job-boards.greenhouse.io/{company}",
        "https://boards.greenhouse.io/{company}"
    ]
    
    # URL patterns for job detail pages, ordered by preference
    JOB_DETAIL_URLS = [
        "https://job-boards.greenhouse.io/embed/job_app?for={company}&token={job_id}",
        "https://boards.greenhouse.io/embed/job_app?for={company}&token={job_id}",
        "https://job-boards.greenhouse.io/{company}/jobs/{job_id}",
        "https://boards.greenhouse.io/{company}/jobs/{job_id}"
    ]
    
    def __init__(self):
        super().__init__()
        self.html_converter = html2text.HTML2Text()
        self.html_converter.ignore_links = False
        self.html_converter.ignore_images = True
        self.html_converter.body_width = 0  # Don't wrap text
        self.html_converter.protect_links = True  # Don't wrap links
        self.html_converter.unicode_snob = True  # Use Unicode
        self.html_converter.ul_item_mark = '-'  # Use - for unordered lists
        # Create two clients - one that follows redirects and one that doesn't
        self.client = httpx.Client(timeout=10.0, follow_redirects=True)
        self.head_client = httpx.Client(timeout=10.0, follow_redirects=False)
    
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

    def _check_url_head(self, url: str) -> Tuple[bool, Optional[str]]:
        """
        Check if a URL is accessible and returns a Greenhouse page.
        Returns (success, final_url).
        """
        try:
            response = self.head_client.head(url)
            
            # If it's a 200, this URL works
            if response.status_code == 200:
                return True, url
                
            # If it's a redirect, check if it's redirecting to a non-Greenhouse domain
            if response.status_code == 302:
                location = response.headers.get('location', '')
                if not any(domain in location.lower() for domain in ['greenhouse.io', 'job-boards.greenhouse.io']):
                    return False, None
                    
            return False, None
            
        except Exception as e:
            logging.debug(f"HEAD request failed for {url}: {str(e)}")
            return False, None
    
    def get_listings_url(self, company_id: str, config: Optional[Dict] = None) -> str:
        """Get the URL for the company's job listings page."""
        config = config or {}
        
        # Check for cached pattern first
        if cached_pattern := config.get('working_url_pattern'):
            try:
                url = cached_pattern.format(company=company_id)
                success, _ = self._check_url_head(url)
                if success:
                    logging.info(f"Using cached URL pattern for {company_id}")
                    return url
                logging.warning(f"Cached URL pattern failed for {company_id}, trying alternatives")
            except Exception as e:
                logging.warning(f"Error with cached URL pattern: {str(e)}")
        
        # Try each pattern in order
        for pattern in self.BASE_URLS:
            try:
                url = pattern.format(company=company_id)
                success, _ = self._check_url_head(url)
                if success:
                    logging.info(f"Found working URL pattern for {company_id}")
                    return url
            except Exception as e:
                logging.debug(f"Pattern {pattern} failed: {str(e)}")
                continue
        
        # If all patterns fail, use first pattern as fallback
        logging.warning(f"No working URL pattern found for {company_id}, using default")
        return self.BASE_URLS[0].format(company=company_id)
    
    def _make_request(self, url: str) -> str:
        """Make a rate-limited request to ScrapingBee."""
        # Acquire rate limit token
        self._rate_limiter.acquire()
        
        # Make the request
        with self.client as client:
            response = client.get(url)
            response.raise_for_status()
            return response.text

    def _extract_job_id(self, href: str) -> Optional[str]:
        """
        Extract job ID from various possible href formats.
        Examples:
        - /company/jobs/1234
        - https://company.com/careers/1234
        - https://boards.greenhouse.io/embed/job_app?for=company&token=1234
        - /embed/job_app?token=1234
        """
        try:
            # Try to extract from token parameter first
            if 'token=' in href:
                token_part = href.split('token=')[-1]
                return token_part.split('&')[0]
            
            # Try to extract from URL path
            parts = href.strip('/').split('/')
            # Get the last part that's numeric
            for part in reversed(parts):
                if part.isdigit():
                    return part
                # Handle if ID is part of a larger string
                numeric_part = ''.join(c for c in part if c.isdigit())
                if numeric_part:
                    return numeric_part
            
            return None
        except Exception as e:
            logging.debug(f"Error extracting job ID from {href}: {str(e)}")
            return None

    def parse_listings_page(self, html_content: str, source_id: str) -> List[JobListing]:
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
                
                # Extract job ID from href
                job_id = self._extract_job_id(job_url)
                if not job_id:
                    logging.error(f"Could not extract job ID from URL: {job_url}")
                    continue
                
                listing = JobListing(
                    source_job_id=job_id,
                    title=title,
                    location=location,
                    department=department,
                    url=self.JOB_DETAIL_URLS[0].format(company=source_id, job_id=job_id),  # Use source_id from company source
                    raw_data={
                        'departments': departments,
                        'office_ids': opening.get('office_id', '').split(','),
                        'source': 'greenhouse',
                        'original_url': job_url,  # Store original URL for reference
                        'scraped_at': datetime.utcnow().isoformat()
                    }
                )
                listings.append(listing)
                
            except (IndexError, AttributeError) as e:
                logging.error(f"Error parsing job listing: {e}")
                continue
                
        return listings
    
    def get_job_detail_url(self, listing: Dict | JobListing, config: Optional[Dict] = None) -> str:
        """Get the URL for a specific job listing."""
        config = config or {}
        
        # Handle both Dict and JobListing objects
        url = listing['url'] if isinstance(listing, dict) else listing.url
        job_id = self._extract_job_id(url)
        
        if not job_id:
            raise ValueError(f"Could not extract job_id from {url}")
        
        # Get company_id from the URL - it should be the source_id that was used to create it
        company_id = self._extract_company_id(url)
        if not company_id:
            raise ValueError(f"Could not extract company_id from {url}")
        
        # Check for cached pattern first
        if cached_pattern := config.get('working_job_detail_pattern'):
            try:
                url = cached_pattern.format(company=company_id, job_id=job_id)
                success, _ = self._check_url_head(url)
                if success:
                    logging.info(f"Using cached job detail pattern for {job_id}")
                    return url
                logging.warning(f"Cached job detail pattern failed for {job_id}, trying alternatives")
            except Exception as e:
                logging.warning(f"Error with cached job detail pattern: {str(e)}")
        
        # Try each pattern in order
        for pattern in self.JOB_DETAIL_URLS:
            try:
                url = pattern.format(company=company_id, job_id=job_id)
                success, _ = self._check_url_head(url)
                if success:
                    # Store working pattern in raw_data for later persistence
                    if isinstance(listing, dict):
                        if 'raw_data' not in listing:
                            listing['raw_data'] = {}
                        if 'config' not in listing['raw_data']:
                            listing['raw_data']['config'] = {}
                        listing['raw_data']['config']['working_job_detail_pattern'] = pattern
                    else:
                        if not hasattr(listing, 'raw_data'):
                            listing.raw_data = {}
                        if 'config' not in listing.raw_data:
                            listing.raw_data['config'] = {}
                        listing.raw_data['config']['working_job_detail_pattern'] = pattern
                    logging.info(f"Found working job detail pattern for {job_id}")
                    return url
            except Exception as e:
                logging.debug(f"Pattern {pattern} failed: {str(e)}")
                continue
        
        # If all patterns fail, use first pattern as fallback
        logging.warning(f"No working job detail pattern found for {job_id}, using default")
        return self.JOB_DETAIL_URLS[0].format(company=company_id, job_id=job_id)

    def get_listing_url(self, listing: Dict | JobListing) -> str:
        """Get the URL for a job listing."""
        return self.get_job_detail_url(listing)

    def _extract_company_id(self, url: str) -> Optional[str]:
        """Extract company ID from URL."""
        try:
            # Try to extract from URL path
            path_match = re.search(r'/([\w-]+)/jobs/', url)
            if path_match:
                return path_match.group(1)
            
            # Try to extract from query parameter
            query_match = re.search(r'for=([\w-]+)', url)
            if query_match:
                return query_match.group(1)
        except Exception as e:
            logging.error(f"Error extracting company ID: {str(e)}")
        
        return None

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

    def parse_job_details(self, html_content: str, job_listing: Dict | JobListing) -> dict:
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
            source_job_id = job_listing['source_job_id'] if isinstance(job_listing, dict) else job_listing.source_job_id
            source_job_id = str(source_job_id)[:255]
            
            # Get existing raw data while preserving non-HTML fields
            existing_raw_data = job_listing['raw_data'] if isinstance(job_listing, dict) else job_listing.raw_data
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
                'url': self.get_listing_url(job_listing),
                'raw_data': raw_data,
                'active': True,
                'first_seen': now,
                'last_seen': now,
                'created_at': now,
                'updated_at': now
            }
            
        except Exception as e:
            logging.error(f"Error parsing job details: {e}")
            source_job_id = job_listing['source_job_id'] if isinstance(job_listing, dict) else job_listing.source_job_id
            return {
                'source_job_id': source_job_id,
                'title': '',
                'location': '',
                'department': None,
                'description': '',
                'url': '',
                'raw_data': {'error': str(e)},
                'active': False
            }
    
    def prepare_scraping_config(self, url: str) -> Dict:
        """Prepare configuration for ScrapingBee API with Greenhouse-specific settings."""
        config = super().prepare_scraping_config(url)
        config['api_key'] = SCRAPING_BEE_API_KEY
        
        if url.count('/') == 3:  # Main listings page
            config['wait_for'] = '//div[contains(@class, "opening")]'
        else:  # Job detail page
            config['wait_for'] = '//div[@id="content"]'
            
        return config 