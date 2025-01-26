"""
Greenhouse job board source implementation.
Uses ScrapingBee for fetching pages and lxml for parsing.
"""
from typing import List, Dict, Optional, Tuple
from lxml import html, etree
import random
import re
from datetime import datetime
import logging

from .base import BaseSource, JobListing

try:
    from config.settings import SCRAPING_BEE_API_KEY
except ImportError:
    SCRAPING_BEE_API_KEY = None  # Will be mocked in tests

class GreenhouseSource(BaseSource):
    """Implementation of BaseSource for Greenhouse job boards."""
    
    # Common section headers for job details
    REQUIREMENTS_HEADERS = [
        "Requirements", 
        "What you'll bring",
        "Skills",
        "Qualifications",
        "Who you are",
        "Skills and Experience",
        "Required Qualifications",
        "Preferred Qualifications",
        "Experience",
        "About You"
    ]
    
    BENEFITS_HEADERS = [
        "Benefits",
        "What we offer",
        "What We Give You",
        "Perks",
        "Why work here",
        "What We Give You",
        "Compensation",
        "Total Rewards",
        "What you'll get",
        "Package"
    ]

    DESCRIPTION_HEADERS = [
        "About the role",
        "Role Overview",
        "Position Summary",
        "Job Summary",
        "The Role",
        "The Opportunity"
    ]

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
        r'(\d{2,3})[k+](?:\s*-\s*(\d{2,3})[k+])?'
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
                        'office_ids': opening.get('office_id', '').split(',')
                    }
                )
                listings.append(listing)
                
            except (IndexError, AttributeError) as e:
                # Log error but continue processing other listings
                print(f"Error parsing job listing: {e}")
                continue
                
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
    
    def _extract_with_fallbacks(self, tree: html.HtmlElement, xpaths: List[str], default: Optional[str] = None) -> Tuple[str, float]:
        """
        Try multiple XPath selectors and return the first match with confidence score.
        
        Args:
            tree: HTML element tree
            xpaths: List of XPath selectors to try
            default: Default value if no matches found
            
        Returns:
            Tuple of (extracted_value, confidence_score)
        """
        for xpath in xpaths:
            elements = tree.xpath(xpath)
            if elements:
                # Higher confidence for earlier xpaths in the list
                confidence = 1.0 - (xpaths.index(xpath) * 0.1)
                return elements[0].strip() if isinstance(elements[0], str) else elements[0].text_content().strip(), confidence
        return default or "", 0.0

    def _extract_section_content(self, tree: html.HtmlElement, section_headers: List[str]) -> Tuple[str, List[str], float]:
        """
        Extract content from a section identified by common headers.
        
        Args:
            tree: HTML element tree
            section_headers: List of possible section header texts
            
        Returns:
            Tuple of (raw_text, structured_items, confidence_score)
        """
        max_confidence = 0.0
        best_content = ""
        structured_items = []
        
        # Try different header elements
        for header_tag in ['h2', 'h3', 'strong', 'b', 'p']:
            for header in section_headers:
                try:
                    # Use a simpler, more robust XPath expression
                    # First try exact match (case-insensitive)
                    header_text = header.lower().replace("'", "''")
                    xpath = f"//{header_tag}[translate(normalize-space(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz')='{header_text}']"
                    elements = tree.xpath(xpath)
                    
                    # If no exact match, try contains with normalized text
                    if not elements:
                        xpath = f"//{header_tag}[contains(normalize-space(), '{header_text}')]"
                        elements = tree.xpath(xpath)
                    
                    for element in elements:
                        # Get the parent section and next sibling sections
                        section = element.getparent()
                        if section is not None:
                            # Try to find the content section
                            content_sections = []
                            current = element.getnext()
                            while current is not None and not any(h.lower() in current.text_content().lower() for h in section_headers):
                                content_sections.append(current)
                                current = current.getnext()
                            
                            # Extract text content
                            content = []
                            # Look for bullet points first
                            for section in content_sections:
                                items = section.xpath('.//li')
                                if items:
                                    new_items = [item.text_content().strip() for item in items]
                                    structured_items.extend(new_items)
                                    content.extend(new_items)
                                    confidence = 1.0  # Highest confidence for structured lists
                                else:
                                    # Fall back to paragraphs
                                    paragraphs = section.xpath('.//p')
                                    if paragraphs:
                                        content.extend(p.text_content().strip() for p in paragraphs)
                                        confidence = 0.8  # Good confidence for paragraphs
                                    else:
                                        # Last resort: all text content
                                        text = section.text_content().strip()
                                        if text:
                                            content.append(text)
                                            confidence = 0.5  # Lower confidence for unstructured text
                            
                            if confidence > max_confidence and content:
                                max_confidence = confidence
                                best_content = '\n'.join(content)
                
                except etree.XPathEvalError as e:
                    logging.warning(f"XPath error for header '{header}' with tag '{header_tag}': {str(e)}")
                    continue
                except Exception as e:
                    logging.warning(f"Unexpected error for header '{header}' with tag '{header_tag}': {str(e)}")
                    continue
        
        return best_content, structured_items, max_confidence

    def _extract_salary_range(self, tree: html.HtmlElement) -> Tuple[Optional[Dict[str, int]], float]:
        """
        Extract salary range information if available.
        
        Args:
            tree: HTML element tree
            
        Returns:
            Tuple of (salary_dict, confidence_score)
        """
        salary_dict = None
        confidence = 0.0
        
        # Try to find salary information in various locations
        salary_containers = [
            "//div[contains(@class, 'pay-range')]",
            "//p[contains(translate(text(), 'SALARY', 'salary'), 'salary')]",
            "//li[contains(translate(text(), 'SALARY', 'salary'), 'salary')]",
            "//div[contains(@class, 'compensation')]"
        ]
        
        for container in salary_containers:
            elements = tree.xpath(container)
            if elements:
                text = elements[0].text_content()
                
                # Try different salary patterns
                for pattern in self.SALARY_PATTERNS:
                    matches = re.findall(pattern, text, re.IGNORECASE)
                    if matches:
                        try:
                            if isinstance(matches[0], tuple):
                                # Range found
                                min_salary = matches[0][0].replace(',', '').replace('k', '000')
                                max_salary = matches[0][1].replace(',', '').replace('k', '000') if matches[0][1] else min_salary
                            else:
                                # Single value found
                                min_salary = max_salary = matches[0].replace(',', '').replace('k', '000')
                            
                            salary_dict = {
                                'min': int(float(min_salary)),
                                'max': int(float(max_salary)),
                                'currency': 'USD' if '$' in text else None
                            }
                            confidence = 1.0 if '$' in text else 0.8
                            break
                        except (ValueError, IndexError):
                            continue
                
                if salary_dict:
                    break
                
        return salary_dict, confidence

    def _extract_employment_info(self, text: str) -> Tuple[Optional[str], Optional[str], float]:
        """
        Extract employment type and remote work status from text.
        
        Args:
            text: Text to analyze
            
        Returns:
            Tuple of (employment_type, remote_status, confidence_score)
        """
        employment_type = None
        remote_status = None
        confidence = 0.0
        
        # Employment type mapping
        employment_map = {
            'full-time': 'FULL_TIME',
            'part-time': 'PART_TIME',
            'contract': 'CONTRACT',
            'internship': 'INTERNSHIP'
        }
        
        # Remote status mapping
        remote_map = {
            'remote': 'REMOTE',
            'hybrid': 'HYBRID',
            'office': 'OFFICE',
            'flexible': 'FLEXIBLE'
        }
        
        # Check employment patterns and normalize
        for pattern in self.EMPLOYMENT_PATTERNS:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                raw_type = match.group(1).lower()
                employment_type = employment_map.get(raw_type, 'UNKNOWN')
                confidence = max(confidence, 0.8)
        
        # Check remote patterns and normalize
        for pattern in self.REMOTE_PATTERNS:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                raw_status = match.group(1).lower()
                remote_status = remote_map.get(raw_status, 'UNKNOWN')
                confidence = max(confidence, 0.8)
        
        return employment_type, remote_status, confidence

    def parse_job_details(self, html_content: str, job_listing: dict | JobListing) -> dict:
        """Parse the job detail page HTML and update the job listing with full details.
        
        Args:
            html_content: The HTML content of the job detail page
            job_listing: The existing job listing with basic info (dict or JobListing)
            
        Returns:
            Dictionary matching Job model schema with parsed details
        """
        tree = html.fromstring(html_content)
        
        # Extract core fields with fallbacks
        title, title_confidence = self._extract_with_fallbacks(tree, [
            '//h1[contains(@class, "app-title")]/text()',
            '//h1[contains(@class, "section-header")]/text()',
            '//meta[@property="og:title"]/@content',
            '//title/text()'
        ])
        
        location, location_confidence = self._extract_with_fallbacks(tree, [
            '//div[contains(@class, "location")]/text()',
            '//meta[@property="og:description"]/@content',
            '//div[contains(@class, "job__location")]//text()',
            '//p[contains(text(), "Location:")]/text()'
        ])
        
        # Extract structured content sections
        description_text = []
        content_div = tree.xpath('//div[contains(@class, "job__description")]')
        if content_div:
            # Get the main description
            description_section, description_items, desc_confidence = self._extract_section_content(
                tree, self.DESCRIPTION_HEADERS
            )
            if description_section:
                description_text.append(description_section)
            else:
                # Fallback to first few paragraphs
                intro_paragraphs = content_div[0].xpath('.//p[position() <= 3]')
                description_text.extend(p.text_content().strip() for p in intro_paragraphs)
        
        # Extract requirements section
        requirements_text, requirements_items, req_confidence = self._extract_section_content(
            tree, self.REQUIREMENTS_HEADERS
        )
        
        # Extract benefits section
        benefits_text, benefits_items, benefits_confidence = self._extract_section_content(
            tree, self.BENEFITS_HEADERS
        )
        
        # Extract salary information
        salary_info, salary_confidence = self._extract_salary_range(tree)
        
        # Try to extract employment type and remote status from all text
        all_text = ' '.join([
            title, location, description_text[0] if description_text else '',
            requirements_text, benefits_text
        ])
        employment_type, remote_status, employment_confidence = self._extract_employment_info(all_text)
        
        # Get source_job_id and ensure proper type
        source_job_id = job_listing.source_job_id if isinstance(job_listing, JobListing) else job_listing['source_job_id']
        source_job_id = str(source_job_id)[:255]
        
        # Get existing raw data while preserving non-HTML fields
        existing_raw_data = job_listing.raw_data if isinstance(job_listing, JobListing) else job_listing.get('raw_data', {})
        raw_data = {k: v for k, v in existing_raw_data.items() if k not in ('html', 'detail_html')}
        
        # Update raw_data with structured information
        raw_data.update({
            'departments': raw_data.get('departments', []),
            'office_ids': raw_data.get('office_ids', []),
            'requirements': requirements_items,
            'benefits': benefits_items,
            'salary_range': salary_info,
            'employment_type': employment_type,
            'remote_status': remote_status,
            'metadata': {
                'scraped_at': datetime.utcnow().isoformat(),
                'source': 'greenhouse',
                'confidence_scores': {
                    'title': title_confidence,
                    'location': location_confidence,
                    'requirements': req_confidence,
                    'benefits': benefits_confidence,
                    'salary': salary_confidence,
                    'employment': employment_confidence
                }
            }
        })
        
        # Get current timestamp for temporal fields
        now = datetime.utcnow()
        
        # Return dictionary matching Job model schema exactly
        return {
            # Required fields
            'source_job_id': source_job_id,
            'title': title[:255],
            'location': location[:255],
            'department': None,  # Will be set by DAG
            'description': '\n'.join(description_text) if description_text else requirements_text,
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
        Adds Greenhouse-specific wait_for selectors based on URL type.
        
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
            config['wait_for'] = '//div[contains(@class, "opening")]'
        else:  # Job detail page
            config['wait_for'] = '//div[@id="content"]'
            
        return config 