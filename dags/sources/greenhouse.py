"""
Greenhouse job board source implementation.
Uses ScrapingBee for fetching pages and lxml for parsing.
"""
from typing import List, Dict, Tuple, Optional
from lxml import html, etree
import re
from datetime import datetime
import logging
import html2text
import time
from threading import Lock
import httpx
from urllib.parse import urlparse, urljoin, parse_qs

from .base import BaseSource, JobListing
from scraping.parsers import (
    TECH_SKILLS,
    SENIORITY_PATTERNS,
    CURRENCY_MAP,
    EMPLOYMENT_PATTERNS,
    REMOTE_PATTERNS,
    SALARY_PATTERNS,
    LOCATION_PATTERNS,
    extract_structured_salary,
    extract_structured_location,
    extract_skills,
    extract_seniority,
    normalize_employment_type,
    normalize_remote_status,
    extract_with_patterns
)

try:
    from config.settings import SCRAPING_BEE_API_KEY
except ImportError:
    SCRAPING_BEE_API_KEY = None  # Will be mocked in tests

class GreenhouseSource(BaseSource):
    """Implementation of BaseSource for Greenhouse job boards."""
    
    # URL patterns for Greenhouse job boards, ordered by preference
    BASE_URLS = [
        "https://boards.greenhouse.io/{company}",
        "https://job-boards.greenhouse.io/{company}",
        "https://boards.greenhouse.io/embed/job_board?for={company}",
        "https://job-boards.greenhouse.io/embed/job_board?for={company}"
    ]
    
    # URL patterns for job detail pages, ordered by preference
    JOB_DETAIL_URLS = [
        "https://job-boards.greenhouse.io/{company}/jobs/{job_id}",
        "https://boards.greenhouse.io/{company}/jobs/{job_id}",
        "https://job-boards.greenhouse.io/embed/job_app?for={company}&token={job_id}",
        "https://boards.greenhouse.io/embed/job_app?for={company}&token={job_id}"
    ]
    
    # Pre-compiled XPath expressions using correct etree.XPath
    JOB_ELEMENT_XPATHS = [
        etree.XPath('//div[contains(@class, "opening")]'),  # Fixed XPath creation
        etree.XPath('//tr[contains(@class, "job-post")]')    # Fixed XPath creation
    ]
    DEPT_HEADER_XPATH = etree.XPath('//h2[@id]|//h3[@id]|//h4[@id]')  # Fixed
    
    def __init__(self):
        super().__init__()
        self.html_converter = html2text.HTML2Text()
        self.html_converter.ignore_links = False
        self.html_converter.ignore_images = True
        self.html_converter.body_width = 0  # Don't wrap text
        self.html_converter.protect_links = True  # Don't wrap links
        self.html_converter.unicode_snob = True  # Use Unicode
        self.html_converter.ul_item_mark = '-'  # Use - for unordered lists
        # Persistent HTTP clients with connection pooling
        self.client = httpx.Client(
            timeout=10.0,
            follow_redirects=True,
            limits=httpx.Limits(max_connections=5, max_keepalive_connections=5)
        )
        self.head_client = httpx.Client(
            timeout=10.0,
            follow_redirects=False,
            limits=httpx.Limits(max_connections=5, max_keepalive_connections=5)
        )
    
    def _check_url_head(self, url: str) -> Tuple[bool, bool]:
        """
        Check if a URL is accessible and returns a Greenhouse page.
        Returns (success, redirects_externally).
        """
        try:
            response = self.head_client.head(url)
            
            # If it's a 200, this URL works
            if response.status_code == 200:
                return True, False
                
            # If it's a redirect, check if it's redirecting to a non-Greenhouse domain
            if response.status_code == 302:
                location = response.headers.get('location', '')
                if not any(domain in location.lower() for domain in ['greenhouse.io', 'job-boards.greenhouse.io']):
                    return False, True
                    
            return False, False
            
        except Exception as e:
            logging.debug(f"HEAD request failed for {url}: {str(e)}")
            return False, False
    
    def get_listings_url(self, company_id: str, config: Optional[Dict] = None) -> Tuple[str, Optional[str]]:
        """
        Get the URL for the company's job listings page.
        Returns (url, working_pattern) tuple.
        """
        config = config or {}
        
        # Check for cached pattern first
        if cached_pattern := config.get('working_url_pattern'):
            try:
                url = cached_pattern.format(company=company_id)
                success, _ = self._check_url_head(url)
                if success:
                    logging.info(f"Using cached URL pattern for {company_id}")
                    return url, cached_pattern
                logging.warning(f"Cached URL pattern failed for {company_id}, trying alternatives")
            except Exception as e:
                logging.warning(f"Error with cached URL pattern: {str(e)}")
        
        # Try each pattern in strict order
        for pattern in self.BASE_URLS:
            try:
                url = pattern.format(company=company_id)
                success, _ = self._check_url_head(url)
                if success:
                    logging.info(f"Found working URL pattern for {company_id}")
                    return url, pattern
            except Exception as e:
                logging.debug(f"Pattern {pattern} failed: {str(e)}")
                continue
        
        # If all patterns fail, use first pattern as fallback
        logging.warning(f"No working URL pattern found for {company_id}, using default")
        return self.BASE_URLS[0].format(company=company_id), None
    
    def _make_request(self, url: str) -> str:
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
        - https://boards.greenhouse.io/company/jobs/1234?gh_jid=1234
        - https://job-boards.greenhouse.io/embed/job_board?for=company/jobs/1234
        """
        try:
            # Split URL into path and query parts
            base_url = href.split('?')[0]
            
            # First try to extract from path
            parts = base_url.strip('/').split('/')
            for part in reversed(parts):
                if part.isdigit() and len(part) > 4:  # Job IDs are typically longer than 4 digits
                    return part
            
            # If not found in path, check URL parameters
            if '?' in href:
                query_part = href.split('?')[1]
                params = dict(param.split('=') for param in query_part.split('&'))
                
                # Check known parameter names
                for param in ['token', 'gh_jid']:
                    if param in params and params[param].isdigit():
                        return params[param]
                
                # Check for job ID in 'for' parameter path
                if 'for' in params:
                    for_parts = params['for'].split('/')
                    for part in reversed(for_parts):
                        if part.isdigit() and len(part) > 4:  # Job IDs are typically longer than 4 digits
                            return part
            
            logging.debug(f"Could not extract job ID from URL using any known pattern: {href}")
            return None
        except Exception as e:
            logging.debug(f"Error extracting job ID from {href}: {str(e)}")
            return None

    def parse_listings_page(self, html_content: str, source_id: str, config: Optional[Dict] = None) -> Tuple[List[JobListing], Dict]:
        """
        Optimized parser using pre-compiled XPaths.
        Returns (listings, updated_config) tuple.
        """
        config = config or {}
        listings = []
        tree = html.fromstring(html_content)
        
        # Get base URL from the page for resolving relative URLs
        base_url = tree.xpath('//base/@href')[0] if tree.xpath('//base/@href') else self.BASE_URLS[0].format(company=source_id)
        
        # Parse base URL properly
        base_url_parsed = urlparse(base_url)
        base_domain = f"{base_url_parsed.scheme}://{base_url_parsed.netloc}"
        is_embed_url = 'embed' in base_url_parsed.path or 'for=' in (base_url_parsed.query or '')

        # Build department ID to name mapping for traditional format
        dept_map = {}
        for dept_header in self.DEPT_HEADER_XPATH(tree):
            dept_id = dept_header.get('id')
            dept_name = dept_header.text_content().strip()
            if dept_id and dept_name:
                dept_map[dept_id] = dept_name
        
        # Use pre-compiled job element XPaths
        job_elements = []
        for xpath in self.JOB_ELEMENT_XPATHS:
            job_elements.extend(xpath(tree))
        
        # Track if we've validated a pattern yet
        validated_pattern = None
        
        for job_element in job_elements:
            try:
                # Handle both formats for job links
                job_links = (
                    job_element.xpath('.//a[@data-mapped]') or  # Traditional format
                    job_element.xpath('.//a')                    # New format
                )
                
                if not job_links:
                    logging.debug("No job link found, skipping listing")
                    continue
                    
                job_link = job_links[0]
                job_url = job_link.get('href')
                
                # Extract job ID from href before any URL manipulation
                job_id = self._extract_job_id(job_url)
                if not job_id:
                    logging.error(f"Could not extract job ID from URL: {job_url}")
                    continue

                # Simplified URL handling:
                if job_url.startswith('http'):
                    # If it's a full URL, use it directly.
                    final_url = job_url
                else:
                    # For non–full URLs, use the JOB_DETAIL_URLS patterns.
                    if validated_pattern is None:
                        # Try cached pattern first.
                        if cached_pattern := config.get('working_job_detail_pattern'):
                            test_url = cached_pattern.format(company=source_id, job_id=job_id)
                            success, _ = self._check_url_head(test_url)
                            if success:
                                validated_pattern = cached_pattern
                                final_url = test_url
                            else:
                                logging.warning(f"Cached job detail pattern failed, trying alternatives")
                        
                        # If no cached pattern or it failed, try patterns in order.
                        if validated_pattern is None:
                            for pattern in self.JOB_DETAIL_URLS:
                                test_url = pattern.format(company=source_id, job_id=job_id)
                                success, _ = self._check_url_head(test_url)
                                if success:
                                    validated_pattern = pattern
                                    final_url = test_url
                                    break
                            
                            # If all patterns fail, use the first pattern as fallback.
                            if validated_pattern is None:
                                logging.warning(f"No working job detail pattern found, using default")
                                validated_pattern = self.JOB_DETAIL_URLS[0]
                                final_url = validated_pattern.format(company=source_id, job_id=job_id)
                    else:
                        # Use already validated pattern.
                        final_url = validated_pattern.format(company=source_id, job_id=job_id)
                
                # Save working pattern to config if we found one
                if validated_pattern and validated_pattern != config.get('working_job_detail_pattern'):
                    config['working_job_detail_pattern'] = validated_pattern

                # Store original URL in raw_data for debugging
                raw_data = {
                    'source': 'greenhouse',
                    'source_id': source_id,
                    'original_url': job_url,  # Store the original href
                    'scraped_at': datetime.utcnow().isoformat()
                }

                # Extract title - handle both formats
                title_elements = (
                    job_link.xpath('.//p[contains(@class, "body--medium")]/text()[1]') or  # New format
                    job_link.xpath('.//text()[not(parent::span[@class="tag-text"])]')  # Traditional format
                )
                # Clean up title
                title = ' '.join(t.strip() for t in title_elements if t.strip())
                title = re.sub(r'\s*New\s*', '', title)  # Remove any "New" text
                title = title.strip()
                
                # Extract location - handle both formats
                location_elements = (
                    job_element.xpath('.//span[@class="location"]/text()') or  # Traditional format
                    job_element.xpath('.//p[contains(@class, "body--metadata")]/text()')  # New format
                )
                location = ' '.join(t.strip() for t in location_elements if t.strip())
                
                # Extract department
                department = None
                # Try traditional format first
                departments = job_element.get('department_id', '').split(',')
                if departments and departments[0]:
                    department = dept_map.get(departments[0], departments[0])
                else:
                    # Multi-format fallback approach
                    dept_header = (
                        job_element.xpath('./ancestor::section[1]/h2[1]/text()') or  # New section/h2 format
                        job_element.xpath('./ancestor::div[contains(@class, "job-posts")][1]/preceding-sibling::h3[contains(@class, "section-header")][1]/text()') or  # Original table format
                        job_element.xpath('./ancestor::*[h2|h3|h4][1]/(h2|h3|h4)[1]/text()')  # Generic fallback
                    )
                    if dept_header:
                        department = dept_header[0].strip()
                
                listing = JobListing(
                    source_job_id=job_id,
                    title=title,
                    location=location,
                    department=department,
                    url=final_url,  # Use validated URL
                    raw_data=raw_data
                )
                listings.append(listing)
                
            except (IndexError, AttributeError) as e:
                logging.error(f"Error parsing job listing: {e}")
                continue
                
        if not listings:
            logging.warning(f"No job listings found for source_id: {source_id}")
            
        return listings, config
    
    def get_job_detail_url(self, listing: Dict | JobListing, config: Optional[Dict] = None) -> Tuple[str, str, Optional[str]]:
        """
        Get the URL for a specific job listing.
        Returns (url, status, pattern) where:
        - url is the best URL to use
        - status is one of: '200' (working), '302' (redirect), 'invalid' (no working URL)
        - pattern is the working pattern if status is '200', otherwise None
        """
        config = config or {}
        
        # Get job_id and company_id
        original_url = listing['url'] if isinstance(listing, dict) else listing.url
        job_id = self._extract_job_id(original_url)
        if not job_id:
            logging.error(f"Could not extract job ID from URL: {original_url}")
            return original_url, 'invalid', None
        
        company_id = None
        if isinstance(listing, dict):
            company_id = listing.get('raw_data', {}).get('source_id')
        else:
            company_id = getattr(listing, 'raw_data', {}).get('source_id')
        
        if not company_id:
            company_id = self._extract_company_id(original_url)
        if not company_id:
            logging.error(f"Could not extract company ID from URL: {original_url}")
            return original_url, 'invalid', None
        
        # If the job's url is already a full URL, use it directly.
        if original_url.startswith('http'):
            return original_url, '200', None

        # For non–full URLs, try each pattern.
        for pattern in self.JOB_DETAIL_URLS:
            try:
                test_url = pattern.format(company=company_id, job_id=job_id)
                success, redirect = self._check_url_head(test_url)
                if success:
                    return test_url, '200', pattern
            except Exception as e:
                logging.debug(f"Pattern {pattern} failed: {str(e)}")
                continue
        
        # If no pattern works, return a fallback using the first pattern.
        fallback_url = self.JOB_DETAIL_URLS[0].format(company=company_id, job_id=job_id)
        return fallback_url, 'invalid', None

    def get_listing_url(self, listing: Dict | JobListing) -> str:
        """Get the URL for a job listing."""
        url, _, _ = self.get_job_detail_url(listing)
        return url

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
        """
        Parse job details from HTML content.
        This method focuses on extracting raw content, leaving structured data parsing to the DAG.
        """
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
            
            description = ""
            if content_elems:
                content_div = content_elems[0]
                description = self._clean_html_content(content_div)
            
            # Get source_job_id from input
            source_job_id = job_listing['source_job_id'] if isinstance(job_listing, dict) else job_listing.source_job_id
            source_job_id = str(source_job_id)[:255]
            
            # Get existing raw data while preserving non-HTML fields
            existing_raw_data = job_listing['raw_data'] if isinstance(job_listing, dict) else job_listing.raw_data
            raw_data = {k: v for k, v in existing_raw_data.items() if k not in ('html', 'detail_html')}
            
            # Get current timestamp for temporal fields
            now = datetime.utcnow()
            
            return {
                'source_job_id': source_job_id,
                'title': title,
                'location': location,
                'department': None,  # Will be set by DAG
                'description': description,
                'url': self.get_listing_url(job_listing),
                'raw_data': raw_data,
                'active': True,
                'first_seen': now,
                'last_seen': now,
                'created_at': now,
                'updated_at': now
            }
            
        except Exception as e:
            logging.error(f"Error parsing job details: {str(e)}")
            return {
                'source_job_id': job_listing['source_job_id'] if isinstance(job_listing, dict) else job_listing.source_job_id,
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