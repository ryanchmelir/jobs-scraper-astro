"""Integration tests for Greenhouse job board scraping.
These tests make actual API calls to ScrapingBee and Greenhouse.
Uses SCRAPING_BEE_API_KEY from config.settings.
Each test costs 1 API credit.
"""
import os
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock
from scrapingbee import ScrapingBeeClient

# Set up Airflow mocks
variable_mock = MagicMock()
variable_mock.get.return_value = os.environ.get('SCRAPING_BEE_API_KEY', 'test-api-key')

base_hook_mock = MagicMock()
conn_mock = MagicMock()
conn_mock.login = 'test_user'
conn_mock.password = 'test_pass'
conn_mock.host = 'localhost'
conn_mock.port = 5432
conn_mock.schema = 'test_db'
base_hook_mock.get_connection.return_value = conn_mock

# Mock Airflow modules before importing settings
with patch.dict('sys.modules', {
    'airflow.hooks.base': MagicMock(BaseHook=base_hook_mock),
    'airflow.models': MagicMock(Variable=variable_mock)
}):
    # Add dags to path for config import
    import sys
    dags_path = Path(__file__).parent.parent.parent.parent / "dags"
    sys.path.insert(0, str(dags_path))

    # Now import settings with mocked Airflow
    from config.settings import SCRAPING_BEE_API_KEY
    from sources.greenhouse import GreenhouseSource

# Skip all tests if no API key
pytestmark = pytest.mark.skipif(
    not SCRAPING_BEE_API_KEY or SCRAPING_BEE_API_KEY == "test-api-key",
    reason="SCRAPING_BEE_API_KEY not set or is default value"
)

@pytest.fixture
def greenhouse_source():
    """Create a GreenhouseSource instance for testing."""
    return GreenhouseSource()

@pytest.fixture
def scraping_client():
    """Create a ScrapingBee client."""
    return ScrapingBeeClient(api_key=SCRAPING_BEE_API_KEY)

@pytest.fixture
def sample_job_listing(greenhouse_source, scraping_client):
    """Get a sample job listing for testing."""
    # Get listings URL for 2U Alumni board
    url = greenhouse_source.get_listings_url("2ualumni")
    
    # Get scraping config
    config = greenhouse_source.prepare_scraping_config(url)
    
    # Make request
    response = scraping_client.get(
        url,
        params={k: v for k, v in config.items() if k != 'api_key'}
    )
    
    # Check response
    assert response.status_code == 200
    
    # Parse listings
    listings = greenhouse_source.parse_listings_page(response.content.decode())
    
    # Verify we got real data
    assert len(listings) > 0
    
    # Return first listing
    return listings[0]

def test_scrape_listings_page(greenhouse_source, scraping_client, sample_job_listing):
    """Test scraping a real Greenhouse job board."""
    # Verify the sample listing has all required fields
    assert sample_job_listing.title
    assert sample_job_listing.location
    assert sample_job_listing.source_job_id
    assert sample_job_listing.url.startswith("https://boards.greenhouse.io")

def test_scrape_job_detail(greenhouse_source, scraping_client, sample_job_listing):
    """Test scraping a job detail page."""
    # Get detail URL
    url = greenhouse_source.get_job_detail_url(sample_job_listing)
    
    # Get scraping config
    config = greenhouse_source.prepare_scraping_config(url)
    
    # Make request
    response = scraping_client.get(
        url,
        params={k: v for k, v in config.items() if k != 'api_key'}
    )
    
    # Check response
    assert response.status_code == 200
    
    # Parse details
    updated_listing = greenhouse_source.parse_job_details(
        response.content.decode(),
        sample_job_listing
    )
    
    # Verify we got detail data
    assert 'full_description' in updated_listing.raw_data
    assert len(updated_listing.raw_data['full_description']) > 0
    assert 'detail_html' in updated_listing.raw_data 