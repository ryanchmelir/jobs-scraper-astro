"""Pytest configuration for job scraper tests."""
import os
import sys
from pathlib import Path

# Add dags directory to Python path for imports
DAGS_DIR = Path(__file__).parent.parent / "dags"
sys.path.insert(0, str(DAGS_DIR.parent)) 