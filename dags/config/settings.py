"""
Environment settings and configuration for the job scraper.
Uses Airflow connections and variables for deployment-specific settings.
"""
from typing import Optional
from pydantic_settings import BaseSettings
from airflow.hooks.base import BaseHook
from airflow.models import Variable
import logging

logger = logging.getLogger(__name__)

class Settings(BaseSettings):
    """
    Settings loaded from Airflow connections/variables.
    Uses Pydantic for validation and typing.
    """
    # Environment (from Airflow variables)
    ENVIRONMENT: str = Variable.get("environment", default_var="development")
    
    # Scraping (from Airflow variables)
    SCRAPING_BEE_API_KEY: str = Variable.get("scraping_bee_api_key")

    model_config = {
        "case_sensitive": True
    }

def get_settings() -> Settings:
    """
    Get validated settings from Airflow.
    Raises helpful errors if required variables are missing.
    """
    try:
        return Settings()
    except Exception as e:
        logger.error(f"Error loading settings: {str(e)}")
        raise

# Global settings instance
settings = get_settings()

# Export commonly used settings
SCRAPING_BEE_API_KEY = settings.SCRAPING_BEE_API_KEY

def get_db_engine():
    """Get SQLAlchemy engine for database access."""
    from sqlalchemy import create_engine
    conn = BaseHook.get_connection("postgres_jobs_db")
    return create_engine(conn.get_uri()) 