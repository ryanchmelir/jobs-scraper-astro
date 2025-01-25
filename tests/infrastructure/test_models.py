"""Integration tests for database models.
Tests model relationships and JobListing conversion.
"""
import pytest
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import StatementError
from infrastructure.models import Base, Company, CompanySource, Job, SourceType
from dags.sources.base import JobListing

@pytest.fixture(scope="function")
def db_engine():
    """Create a test database."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return engine

@pytest.fixture(scope="function")
def db_session(db_engine):
    """Create a new database session for a test."""
    Session = sessionmaker(bind=db_engine)
    session = Session()
    yield session
    session.close()

@pytest.fixture
def test_company(db_session):
    """Create a test company."""
    company = Company(name="Test Company")
    db_session.add(company)
    db_session.commit()
    return company

@pytest.fixture
def test_source(db_session, test_company):
    """Create a test company source."""
    source = CompanySource(
        company=test_company,
        source_type=SourceType.GREENHOUSE,
        source_id="test-source",
        active=True,
        scrape_interval=60,
        next_scrape_time=datetime.utcnow() + timedelta(minutes=60)
    )
    db_session.add(source)
    db_session.commit()
    return source

def test_company_relationships(db_session, test_company, test_source):
    """Test company relationships with sources and jobs."""
    # Test company -> source relationship
    assert len(test_company.sources) == 1
    assert test_company.sources[0].source_id == "test-source"
    
    # Add a job
    job = Job(
        company=test_company,
        company_source=test_source,
        source_job_id="test-job-1",
        title="Test Job"
    )
    db_session.add(job)
    db_session.commit()
    
    # Test relationships
    assert len(test_company.jobs) == 1
    assert test_company.jobs[0].title == "Test Job"
    assert len(test_source.jobs) == 1
    assert test_source.jobs[0].source_job_id == "test-job-1"

def test_source_type_enum(db_session, test_company):
    """Test SourceType enumeration."""
    # Test valid source type
    source = CompanySource(
        company=test_company,
        source_type=SourceType.GREENHOUSE,
        source_id="test"
    )
    db_session.add(source)
    db_session.commit()
    assert source.id is not None
    assert source.source_type == SourceType.GREENHOUSE
    assert source.source_type.value == "greenhouse"
    
    # Test that source type is loaded correctly from DB
    db_session.expunge_all()
    loaded_source = db_session.query(CompanySource).filter_by(id=source.id).first()
    assert loaded_source.source_type == SourceType.GREENHOUSE
    assert isinstance(loaded_source.source_type, SourceType)
    
    # Test all enum values
    assert list(SourceType) == [SourceType.GREENHOUSE]
    assert SourceType.GREENHOUSE.value == "greenhouse"

def test_unique_constraints(db_session, test_company, test_source):
    """Test unique constraints on models."""
    # Test unique source_type + source_id
    duplicate_source = CompanySource(
        company=test_company,
        source_type=test_source.source_type,
        source_id=test_source.source_id
    )
    db_session.add(duplicate_source)
    with pytest.raises(Exception):  # SQLite raises IntegrityError
        db_session.commit()
    db_session.rollback()
    
    # Test unique company_source + source_job_id
    job1 = Job(
        company=test_company,
        company_source=test_source,
        source_job_id="job-1",
        title="Job 1"
    )
    db_session.add(job1)
    db_session.commit()
    
    job2 = Job(
        company=test_company,
        company_source=test_source,
        source_job_id="job-1",  # Same ID
        title="Job 2"
    )
    db_session.add(job2)
    with pytest.raises(Exception):
        db_session.commit()

def test_job_listing_to_model(db_session, test_company, test_source):
    """Test converting JobListing to Job model."""
    # Create a JobListing
    listing = JobListing(
        source_job_id="test-job",
        title="Software Engineer",
        location="Remote",
        department="Engineering",
        url="https://example.com/jobs/test-job",
        raw_data={"description": "Test description"}
    )
    
    # Convert to Job model
    job = Job(
        company=test_company,
        company_source=test_source,
        source_job_id=listing.source_job_id,
        title=listing.title,
        location=listing.location,
        department=listing.department,
        raw_data=listing.raw_data
    )
    
    # Save and verify
    db_session.add(job)
    db_session.commit()
    
    # Reload from database
    saved_job = db_session.query(Job).filter_by(source_job_id=listing.source_job_id).first()
    assert saved_job is not None
    assert saved_job.title == listing.title
    assert saved_job.location == listing.location
    assert saved_job.department == listing.department
    assert saved_job.raw_data == listing.raw_data
    
    # Verify timestamps
    assert saved_job.created_at is not None
    assert saved_job.updated_at is not None
    assert saved_job.first_seen is not None
    assert saved_job.last_seen is not None

def test_active_flags(db_session, test_company, test_source):
    """Test active flags on models."""
    # Test company active flag
    test_company.active = False
    db_session.commit()
    company = db_session.query(Company).filter_by(id=test_company.id).first()
    assert not company.active
    
    # Test source active flag
    test_source.active = False
    db_session.commit()
    source = db_session.query(CompanySource).filter_by(id=test_source.id).first()
    assert not source.active
    
    # Test job active flag
    job = Job(
        company=test_company,
        company_source=test_source,
        source_job_id="test-job",
        title="Test Job",
        active=False
    )
    db_session.add(job)
    db_session.commit()
    saved_job = db_session.query(Job).filter_by(id=job.id).first()
    assert not saved_job.active 