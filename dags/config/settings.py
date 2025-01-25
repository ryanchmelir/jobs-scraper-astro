"""
Environment settings and configuration for the job scraper.
Uses Airflow connections and variables for deployment-specific settings.
Uses environment variables for application-specific settings.
"""
from typing import Optional
from pydantic import BaseSettings, PostgresDsn
from airflow.hooks.base import BaseHook
from airflow.models import Variable
import logging

logger = logging.getLogger(__name__)

class Settings(BaseSettings):
    """
    Settings loaded from Airflow connections/variables and environment variables.
    Uses Pydantic for validation and typing.
    """
    # Environment (from Airflow variables)
    ENVIRONMENT: str = Variable.get("environment", default_var="development")
    
    # Scraping (from environment)
    SCRAPING_BEE_API_KEY: str
    
    # Database (from Airflow connection)
    POSTGRES_DSN: Optional[PostgresDsn] = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._init_postgres_dsn()

    def _init_postgres_dsn(self):
        """Initialize PostgreSQL DSN from Airflow connection."""
        try:
            conn = BaseHook.get_connection("postgres_jobs_db")
            # Build DSN from connection parts
            self.POSTGRES_DSN = PostgresDsn.build(
                scheme="postgresql",
                username=conn.login,
                password=conn.password,
                host=conn.host,
                port=int(conn.port) if conn.port else 5432,
                path=f"/{conn.schema}" if conn.schema else ""
            )
        except Exception as e:
            logger.error(f"Error getting database connection: {str(e)}")
            raise ValueError("Database connection 'postgres_jobs_db' not found or invalid")
    
    class Config:
        case_sensitive = True
        env_file = ".env"

def get_settings() -> Settings:
    """
    Get validated settings from Airflow and environment.
    Raises helpful errors if required variables are missing.
    """
    try:
        return Settings()
    except Exception as e:
        logger.error(f"Error loading settings: {str(e)}")
        raise

# Global settings instance
settings = get_settings() 