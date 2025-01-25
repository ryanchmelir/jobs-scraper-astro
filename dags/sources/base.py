"""
Base interface for job board sources.
All source implementations must inherit from BaseSource.
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime

@dataclass(frozen=True)
class JobListing:
    """
    Represents a job listing from any source.
    Immutable and hashable for safe use in sets and as dict keys.
    """
    source_job_id: str
    title: str
    location: Optional[str] = None
    department: Optional[str] = None
    url: Optional[str] = None
    raw_data: Optional[Dict] = field(default=None, compare=False)  # Exclude from equality

    def __hash__(self):
        """Hash based on source_job_id which is unique."""
        return hash(self.source_job_id)

class BaseSource(ABC):
    """
    Abstract base class for job board sources.
    Each source (Greenhouse, Lever, etc.) must implement these methods.
    """
    
    @abstractmethod
    def get_listings_url(self, company_source_id: int) -> str:
        """
        Get the URL for the company's job listings page.
        
        Args:
            company_source_id: ID of the company source record
            
        Returns:
            URL string for the job listings page
        """
        pass
    
    @abstractmethod
    def parse_listings_page(self, html: str) -> List[JobListing]:
        """
        Parse the job listings page HTML into JobListing objects.
        
        Args:
            html: Raw HTML from the job listings page
            
        Returns:
            List of JobListing objects
        """
        pass
    
    @abstractmethod
    def get_job_detail_url(self, job_listing: JobListing) -> str:
        """
        Get the URL for a specific job's detail page.
        
        Args:
            job_listing: JobListing object with basic job info
            
        Returns:
            URL string for the job detail page
        """
        pass
    
    @abstractmethod
    def parse_job_details(self, html: str, job_listing: JobListing) -> JobListing:
        """
        Parse the job detail page HTML and update the JobListing.
        
        Args:
            html: Raw HTML from the job detail page
            job_listing: Existing JobListing to update with details
            
        Returns:
            Updated JobListing with full details
        """
        pass
    
    def prepare_scraping_config(self, url: str) -> Dict:
        """
        Prepare configuration for ScrapingBee API.
        Uses minimal settings by default to save API credits.
        Can be overridden by sources if needed.
        
        Args:
            url: The URL to be scraped
            
        Returns:
            Dictionary of parameters for ScrapingBee
        """
        return {
            'url': url,
            'render_js': False,  # Default to raw HTML to save credits
            'block_resources': True,  # Block images/CSS to save bandwidth
            'country_code': 'us',  # Use US proxy to avoid region blocks
            'timeout': 20000,  # 20 second timeout
            'transparent_status_code': True  # Get actual status codes
        } 