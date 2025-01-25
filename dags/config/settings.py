"""
Environment settings and configuration for the job scraper.
Uses Airflow connections and variables for deployment-specific settings.
"""
from typing import Optional
from pydantic import PostgresDsn
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
    
    # Database (from Airflow connection)
    POSTGRES_DSN: Optional[PostgresDsn] = None

    model_config = {
        "case_sensitive": True
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._init_postgres_dsn()

    def _init_postgres_dsn(self):
        """Initialize PostgreSQL DSN from Airflow connection."""
        try:
            conn = BaseHook.get_connection("postgres_jobs_db")
            logger.info(f"Building DSN with host={conn.host}, schema={conn.schema}")
            
            # Build DSN from connection parts, handling schema correctly
            dsn_parts = {
                "scheme": "postgresql",
                "username": conn.login,
                "password": conn.password,
                "host": conn.host,
                "port": int(conn.port) if conn.port else 5432,
            }
            
            # Only add schema if it exists, without leading slash
            if conn.schema:
                dsn_parts["path"] = conn.schema
            
            # Add SSL mode if specified in extras
            if conn.extra_dejson.get("sslmode"):
                dsn_parts["query"] = f"sslmode={conn.extra_dejson['sslmode']}"
            
            self.POSTGRES_DSN = PostgresDsn.build(**dsn_parts)
            logger.info("Successfully built PostgreSQL DSN")
            
        except Exception as e:
            logger.error(f"Error getting database connection: {str(e)}")
            raise ValueError("Database connection 'postgres_jobs_db' not found or invalid")

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
    return create_engine(str(settings.POSTGRES_DSN)) 