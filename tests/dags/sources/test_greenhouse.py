"""Tests for the Greenhouse job board source implementation."""
import os
import pytest
from pathlib import Path
from unittest.mock import patch

from dags.sources.greenhouse import GreenhouseSource
from dags.sources.base import JobListing

@pytest.fixture
def greenhouse_source():
    """Create a GreenhouseSource instance for testing."""
    with patch('dags.sources.greenhouse.SCRAPING_BEE_API_KEY', 'test-api-key'):
        yield GreenhouseSource()

@pytest.fixture
def sample_listings_html():
    """Load sample Greenhouse listings HTML."""
    examples_dir = Path(os.environ.get("AIRFLOW_HOME", "")).parent / "examples"
    html_path = examples_dir / "greenhouse/company-job-listings/raw/page.html"
    return html_path.read_text()

def test_get_listings_url(greenhouse_source):
    """Test generating the job board URL."""
    url = greenhouse_source.get_listings_url("testcompany")
    assert url == "https://boards.greenhouse.io/testcompany"

def test_parse_listings_page(greenhouse_source, sample_listings_html):
    """Test parsing job listings from HTML."""
    listings = greenhouse_source.parse_listings_page(sample_listings_html)
    
    # Verify we got listings
    assert len(listings) > 0
    
    # Check first listing has required fields
    first_listing = listings[0]
    assert isinstance(first_listing, JobListing)
    assert first_listing.title
    assert first_listing.location
    assert first_listing.source_job_id
    assert first_listing.url.startswith("https://boards.greenhouse.io")
    
    # Verify raw data is captured
    assert 'departments' in first_listing.raw_data
    assert 'office_ids' in first_listing.raw_data
    assert 'html' in first_listing.raw_data

def test_get_job_detail_url(greenhouse_source):
    """Test generating job detail URL."""
    listing = JobListing(
        source_job_id="123",
        title="Test Job",
        url="https://boards.greenhouse.io/company/jobs/123"
    )
    url = greenhouse_source.get_job_detail_url(listing)
    assert url == "https://boards.greenhouse.io/company/jobs/123"

def test_prepare_scraping_config(greenhouse_source):
    """Test ScrapingBee configuration."""
    url = "https://boards.greenhouse.io/testcompany"
    config = greenhouse_source.prepare_scraping_config(url)
    
    # Check essential config
    assert config['api_key'] == 'test-api-key'
    assert config['url'] == url
    assert config['render_js'] is False
    assert config['country_code'] == 'us'
    assert config['block_resources'] is True
    assert config['transparent_status_code'] is True
    
    # Verify premium proxy is disabled
    assert 'premium_proxy' not in config 