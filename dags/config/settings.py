"""
Environment settings and configuration for the job scraper.
Uses Airflow variables for deployment-specific settings.
"""
from airflow.models import Variable

# Environment settings from Airflow variables
ENVIRONMENT = Variable.get("environment", default_var="development")
SCRAPING_BEE_API_KEY = Variable.get("scraping_bee_api_key") 