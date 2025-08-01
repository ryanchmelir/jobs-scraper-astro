from sqlalchemy import Column, Integer, String, DateTime, Boolean, ForeignKey, JSON, Text, Enum as SQLEnum
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime
from enum import Enum
import sqlalchemy as sa

Base = declarative_base()

class SourceType(str, Enum):
    """Enumeration of supported job board sources"""
    GREENHOUSE = "greenhouse"

class EmploymentType(str, Enum):
    """Enumeration of employment types"""
    FULL_TIME = "FULL_TIME"
    PART_TIME = "PART_TIME"
    CONTRACT = "CONTRACT"
    INTERNSHIP = "INTERNSHIP"
    UNKNOWN = "UNKNOWN"

class RemoteStatus(str, Enum):
    """Enumeration of remote work statuses"""
    REMOTE = "REMOTE"
    HYBRID = "HYBRID"
    OFFICE = "OFFICE"
    FLEXIBLE = "FLEXIBLE"
    UNKNOWN = "UNKNOWN"

class CompanySourceIssues(Base):
    """Tracks failure counts for company sources."""
    __tablename__ = 'company_source_issues'
    
    id = Column(Integer, primary_key=True)
    company_source_id = Column(Integer, ForeignKey('company_sources.id'), nullable=False)
    failure_count = Column(Integer, default=0)
    last_failure = Column(DateTime, nullable=True)
    
    # Relationship
    company_source = relationship("CompanySource", back_populates="issues")

class CompanySource(Base):
    __tablename__ = 'company_sources'
    
    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey('companies.id'), nullable=False)
    source_type = Column(SQLEnum(SourceType), nullable=False)
    source_id = Column(String(255), nullable=False)  # The ID for this source (e.g. greenhouse_id)
    config = Column(JSON)  # Source-specific configuration
    active = Column(Boolean, default=True)
    last_scraped = Column(DateTime)
    next_scrape_time = Column(DateTime)
    scrape_interval = Column(Integer, default=1440)
    
    # Relationships
    company = relationship("Company", back_populates="sources")
    jobs = relationship("Job", back_populates="company_source")
    issues = relationship("CompanySourceIssues", back_populates="company_source", uselist=False)
    
    __table_args__ = (
        sa.UniqueConstraint('source_type', 'source_id', name='uix_source_id'),
    )

class Company(Base):
    __tablename__ = 'companies'

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    jobs = relationship("Job", back_populates="company")
    sources = relationship("CompanySource", back_populates="company")

    def __repr__(self):
        return f"<Company(name='{self.name}')>"

class Job(Base):
    """
    Represents a job listing in the database.
    Column order matches the actual database structure.
    """
    __tablename__ = 'jobs'

    # Primary key
    id = Column(Integer, primary_key=True)
    
    # Foreign keys and basic info (matches DB order)
    company_id = Column(Integer, ForeignKey('companies.id'), nullable=False)
    title = Column(String(255), nullable=False)
    location = Column(String(255))
    department = Column(String(255))
    description = Column(Text)
    url = Column(String(1024))  # Job posting URL
    raw_data = Column(JSON)
    active = Column(Boolean, default=True)
    
    # Temporal fields
    first_seen = Column(DateTime, nullable=False)
    last_seen = Column(DateTime, nullable=False)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    
    # Source tracking
    company_source_id = Column(Integer, ForeignKey('company_sources.id'), nullable=False)
    source_job_id = Column(String(255), nullable=False)
    
    # Salary information
    salary_min = Column(Integer)
    salary_max = Column(Integer)
    salary_currency = Column(String(3))  # ISO currency code
    
    # Employment details
    employment_type = Column(SQLEnum(EmploymentType), default=EmploymentType.UNKNOWN)
    remote_status = Column(SQLEnum(RemoteStatus), default=RemoteStatus.UNKNOWN)
    
    # Relationships
    company = relationship('Company', back_populates='jobs')
    company_source = relationship('CompanySource', back_populates='jobs')

    __table_args__ = (
        sa.UniqueConstraint('company_source_id', 'source_job_id', name='unique_job_per_source'),
    )

    def __repr__(self):
        return f"<Job(title='{self.title}', company_id={self.company_id}, source_job_id='{self.source_job_id}')>" 