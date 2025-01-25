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
            logger.info("Attempting to get postgres_jobs_db connection")
            conn = BaseHook.get_connection("postgres_jobs_db")
            logger.info(f"Connection details: host={conn.host}, port={conn.port}, schema={conn.schema}, extras={conn.extra_dejson}")
            
            # Build DSN from connection parts, handling schema correctly
            dsn_parts = {
                "scheme": "postgresql",
                "username": conn.login,
                "password": conn.password,
                "host": conn.host,
                "port": int(conn.port) if conn.port else 5432,
            }
            logger.info(f"Built initial DSN parts: {dsn_parts}")
            
            # Add schema with leading slash
            if conn.schema:
                dsn_parts["path"] = f"/{conn.schema}"  # Add leading slash
                logger.info(f"Added schema to path: {dsn_parts['path']}")
            
            # Add SSL mode if specified in extras
            if conn.extra_dejson.get("sslmode"):
                dsn_parts["query"] = f"sslmode={conn.extra_dejson['sslmode']}"
                logger.info(f"Added SSL mode to query: {dsn_parts['query']}")
            
            logger.info(f"Final DSN parts before build: {dsn_parts}")
            self.POSTGRES_DSN = PostgresDsn.build(**dsn_parts)
            logger.info(f"Successfully built PostgreSQL DSN: {self.POSTGRES_DSN}")
            
        except Exception as e:
            logger.error(f"Error getting database connection: {str(e)}")
            logger.error(f"Error type: {type(e)}")
            logger.error(f"Error args: {e.args}")
            raise ValueError(f"Database connection 'postgres_jobs_db' not found or invalid: {str(e)}")

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