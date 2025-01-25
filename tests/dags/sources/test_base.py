"""Unit tests for base job source functionality."""
import pytest
from dags.sources.base import JobListing, BaseSource

def test_job_listing_creation():
    """Test creating JobListing objects with various fields."""
    # Test minimal required fields
    listing = JobListing(
        source_job_id="123",
        title="Software Engineer"
    )
    assert listing.source_job_id == "123"
    assert listing.title == "Software Engineer"
    assert listing.location is None
    assert listing.department is None
    assert listing.url is None
    assert listing.raw_data is None
    
    # Test all fields
    listing = JobListing(
        source_job_id="456",
        title="Product Manager",
        location="Remote",
        department="Product",
        url="https://example.com/jobs/456",
        raw_data={"some": "data"}
    )
    assert listing.source_job_id == "456"
    assert listing.title == "Product Manager"
    assert listing.location == "Remote"
    assert listing.department == "Product"
    assert listing.url == "https://example.com/jobs/456"
    assert listing.raw_data == {"some": "data"}

class TestSource(BaseSource):
    """Test implementation of BaseSource."""
    def get_listings_url(self, company_source_id: int) -> str:
        return f"https://example.com/jobs/{company_source_id}"
    
    def parse_listings_page(self, html: str) -> list[JobListing]:
        return [
            JobListing(
                source_job_id="123",
                title="Test Job"
            )
        ]
    
    def get_job_detail_url(self, job_listing: JobListing) -> str:
        return f"https://example.com/jobs/detail/{job_listing.source_job_id}"
    
    def parse_job_details(self, html: str, job_listing: JobListing) -> JobListing:
        # Create new instance with updated raw_data
        return JobListing(
            source_job_id=job_listing.source_job_id,
            title=job_listing.title,
            location=job_listing.location,
            department=job_listing.department,
            url=job_listing.url,
            raw_data={"description": "Test description"}
        )

def test_base_source_implementation():
    """Test that BaseSource can be implemented correctly."""
    source = TestSource()
    
    # Test get_listings_url
    url = source.get_listings_url(789)
    assert url == "https://example.com/jobs/789"
    
    # Test parse_listings_page
    listings = source.parse_listings_page("<html>test</html>")
    assert len(listings) == 1
    assert listings[0].source_job_id == "123"
    assert listings[0].title == "Test Job"
    
    # Test get_job_detail_url
    job = JobListing(source_job_id="456", title="Test Job")
    detail_url = source.get_job_detail_url(job)
    assert detail_url == "https://example.com/jobs/detail/456"
    
    # Test parse_job_details
    updated_job = source.parse_job_details("<html>test</html>", job)
    assert updated_job.source_job_id == job.source_job_id  # Same ID
    assert updated_job.title == job.title  # Same title
    assert updated_job.raw_data == {"description": "Test description"}  # Updated data

def test_default_scraping_config():
    """Test the default scraping configuration."""
    source = TestSource()
    url = "https://example.com/test"
    config = source.prepare_scraping_config(url)
    
    assert config["url"] == url
    assert config["render_js"] is True
    assert config["premium_proxy"] is True

def test_base_source_abstract_methods():
    """Test that BaseSource cannot be instantiated without implementing abstract methods."""
    with pytest.raises(TypeError):
        BaseSource()

def test_job_listing_immutability():
    """Test that JobListing fields can't be modified after creation."""
    listing = JobListing(
        source_job_id="123",
        title="Software Engineer"
    )
    
    # Verify we can't modify fields
    with pytest.raises(AttributeError):
        listing.source_job_id = "456"
    
    with pytest.raises(AttributeError):
        listing.title = "New Title"

def test_job_listing_equality():
    """Test that JobListing objects are compared correctly."""
    listing1 = JobListing(
        source_job_id="123",
        title="Software Engineer",
        location="Remote"
    )
    
    listing2 = JobListing(
        source_job_id="123",
        title="Software Engineer",
        location="Remote"
    )
    
    listing3 = JobListing(
        source_job_id="456",
        title="Software Engineer",
        location="Remote"
    )
    
    assert listing1 == listing2  # Same data should be equal
    assert listing1 != listing3  # Different IDs should not be equal
    assert hash(listing1) == hash(listing2)  # Same data should have same hash 