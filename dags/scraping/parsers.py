"""
Shared parsing logic for job listings data.
This module contains standardized functions for parsing and cleaning job-related data
like salaries, locations, employment types, etc.
"""
from typing import Dict, List, Optional, Tuple
import re
from datetime import datetime
from enum import Enum

# Common tech skills by category
TECH_SKILLS = {
    'programming': [
        'python', 'java', 'javascript', 'typescript', 'c\+\+', 'c#', 'ruby', 'go', 'rust',
        'php', 'scala', 'kotlin', 'swift', 'objective-c', 'perl', 'r programming'
    ],
    'data': [
        'sql', 'mysql', 'postgresql', 'mongodb', 'elasticsearch', 'redis', 'cassandra',
        'hadoop', 'spark', 'tableau', 'power bi', 'looker', 'pandas', 'numpy',
        'scikit-learn', 'tensorflow', 'pytorch', 'machine learning', 'ai', 'nlp',
        'data mining', 'etl', 'data warehouse', 'data lake', 'snowflake', 'redshift'
    ],
    'web': [
        'html', 'css', 'react', 'angular', 'vue', 'node\.js', 'express', 'django',
        'flask', 'spring', 'asp\.net', 'ruby on rails', 'graphql', 'rest api'
    ],
    'cloud': [
        'aws', 'azure', 'gcp', 'docker', 'kubernetes', 'terraform', 'ansible',
        'jenkins', 'circleci', 'github actions', 'devops', 'sre', 'cloud native'
    ],
    'tools': [
        'git', 'jira', 'confluence', 'slack', 'agile', 'scrum', 'kanban',
        'ci/cd', 'testing', 'debugging', 'monitoring', 'logging'
    ]
}

# Seniority levels and their variations
SENIORITY_PATTERNS = [
    (r'\b(principal|staff|lead|architect)\b', 'Principal/Staff'),
    (r'\b(senior|sr\.?|experienced)\b', 'Senior'),
    (r'\b(mid|intermediate)\b', 'Mid-Level'),
    (r'\b(junior|jr\.?|entry[- ]?level|associate)\b', 'Junior'),
    (r'\b(intern|internship|co-op)\b', 'Intern')
]

# Currency codes and symbols
CURRENCY_MAP = {
    '$': 'USD',
    '£': 'GBP',
    '€': 'EUR',
    '¥': 'JPY',
    'USD': 'USD',
    'GBP': 'GBP',
    'EUR': 'EUR',
    'JPY': 'JPY'
}

# Pattern collections
EMPLOYMENT_PATTERNS = [
    r'(full[- ]time|part[- ]time)',
    r'(permanent|contract|temporary)',
    r'(internship|co-op)',
    r'(\d+|full)[- ]time equivalent'
]

REMOTE_PATTERNS = [
    r'(remote|hybrid|office[- ]first)',
    r'(in[- ]office|on[- ]site)',
    r'(work[- ]from[- ]home|wfh)',
    r'(\d+\s*days?\s*(in|remote))'
]

SALARY_PATTERNS = [
    r'\$?(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)[k+]?(?:\s*[-–]\s*\$?(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)[k+]?)?',
    r'~\s*\$?(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)[k+]?',
    r'Salary:\s*\$?(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)[k+]?',
    r'(\d{2,3})[k+](?:\s*-\s*(\d{2,3})[k+])?',
    r'(?:USD|EUR|GBP)?\s*\$?(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)',
    r'compensation.*?\$(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)',
    r'range.*?\$(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)'
]

LOCATION_PATTERNS = [
    r'((?:remote|hybrid)\s+in\s+[^\.;]+)',
    r'((?:based|located)\s+in\s+[^\.;]+)',
    r'(location:\s*[^\.;]+)',
    r'([A-Z][a-zA-Z\s]+,\s+[A-Z]{2})',  # City, State
    r'([A-Z][a-zA-Z\s]+,\s+[A-Z][a-zA-Z\s]+)'  # City, Country
]

def parse_salary_string(salary_str: Optional[str]) -> Tuple[Optional[int], Optional[int], Optional[str]]:
    """Parse a salary string into min, max, and currency."""
    if not salary_str:
        return None, None, None
        
    # Remove any whitespace and convert to lowercase
    salary_str = salary_str.lower().strip()
    
    # Try to identify currency
    currency = 'USD'  # Default
    if '€' in salary_str:
        currency = 'EUR'
    elif '£' in salary_str:
        currency = 'GBP'
    
    # Extract numbers
    numbers = re.findall(r'(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)', salary_str)
    if not numbers:
        return None, None, None
        
    # Convert numbers to integers
    clean_numbers = []
    for num in numbers:
        # Remove commas and handle 'k' multiplier
        num = num.replace(',', '')
        if 'k' in salary_str:
            num = float(num) * 1000
        clean_numbers.append(int(float(num)))
    
    if len(clean_numbers) == 1:
        return clean_numbers[0], clean_numbers[0], currency
    elif len(clean_numbers) >= 2:
        return min(clean_numbers), max(clean_numbers), currency
    
    return None, None, currency

def extract_structured_salary(text: str) -> Dict:
    """
    Extract structured salary information from text with improved accuracy.
    Uses a multi-stage approach with pattern prioritization and validation.
    """
    result = {
        'amount_min': None,
        'amount_max': None,
        'currency': None,
        'interval': 'yearly',  # Default to yearly
        'raw_text': text
    }
    
    if not text:
        return result

    def parse_amount(amount_str: str) -> Optional[float]:
        """Parse salary amount handling k suffix, commas, and currency symbols."""
        try:
            # Remove currency symbols and commas
            clean = re.sub(r'[$£€¥]', '', amount_str)
            clean = clean.replace(',', '')
            
            # Handle k/K suffix
            multiplier = 1000 if re.search(r'k|K', clean) else 1
            clean = re.sub(r'k|K', '', clean)
            
            # Convert to float and apply multiplier
            return float(clean) * multiplier
        except:
            return None

    def validate_salary(amount: float, context: str = '') -> bool:
        """
        Validate if a number looks like a reasonable salary.
        Also checks surrounding context to avoid employee counts.
        """
        # Basic range check for annual salaries
        if not (20000 <= amount <= 1000000):
            return False
            
        # Check if number appears in employee count context
        employee_patterns = [
            r'employees?',
            r'staff',
            r'team\s+size',
            r'company\s+size',
            r'people'
        ]
        context_window = 50  # Characters to check before/after
        start_idx = max(0, context.find(str(int(amount))) - context_window)
        end_idx = min(len(context), context.find(str(int(amount))) + context_window)
        surrounding_text = context[start_idx:end_idx].lower()
        
        if any(re.search(pattern, surrounding_text) for pattern in employee_patterns):
            return False
            
        return True

    def find_currency(text: str) -> str:
        """Identify currency from text."""
        currency_patterns = {
            r'(?:USD|US dollars?|American dollars?)': 'USD',
            r'(?:EUR|euros?)': 'EUR',
            r'(?:GBP|pounds? sterling|£)': 'GBP',
            r'(?:CAD|Canadian dollars?)': 'CAD',
            r'(?:AUD|Australian dollars?)': 'AUD'
        }
        
        # First check for explicit currency mentions
        for pattern, currency in currency_patterns.items():
            if re.search(pattern, text, re.I):
                return currency
                
        # Then check for symbols
        if '£' in text:
            return 'GBP'
        elif '€' in text:
            return 'EUR'
        elif '¥' in text:
            return 'JPY'
        elif '$' in text:
            # Try to distinguish between different dollar types
            if any(term in text.lower() for term in ['cad', 'canada']):
                return 'CAD'
            elif any(term in text.lower() for term in ['aud', 'australia']):
                return 'AUD'
            return 'USD'  # Default to USD for $ symbol
            
        return 'USD'  # Default currency

    def find_interval(text: str) -> str:
        """Identify salary interval."""
        if re.search(r'per\s+hour|hourly|/\s*hr|/\s*hour', text, re.I):
            return 'hourly'
        elif re.search(r'per\s+month|monthly|/\s*month', text, re.I):
            return 'monthly'
        elif re.search(r'per\s+week|weekly|/\s*week', text, re.I):
            return 'weekly'
        return 'yearly'

    # Try patterns in order of reliability
    patterns = [
        # High confidence: Structured formats
        (r'<div[^>]*class="[^"]*pay[^"]*"[^>]*>\s*(?:[\w\s]*?:)?\s*(\$?[\d,.]+[kK]?)\s*[-–~to]+\s*(\$?[\d,.]+[kK]?)', 'high'),
        (r'salary\s*range\s*(?:is|:)?\s*(\$?[\d,.]+[kK]?)\s*[-–~to]+\s*(\$?[\d,.]+[kK]?)', 'high'),
        (r'compensation\s*(?:range|band)?\s*(?:is|:)?\s*(\$?[\d,.]+[kK]?)\s*[-–~to]+\s*(\$?[\d,.]+[kK]?)', 'high'),
        
        # Medium confidence: Contextual patterns
        (r'(?:salary|compensation|pay)\s*(?:range|band)?\s*(?:is|:)?\s*(\$?[\d,.]+[kK]?)\s*[-–~to]+\s*(\$?[\d,.]+[kK]?)', 'medium'),
        (r'(?:range|band)\s*(?:is|:)?\s*(\$?[\d,.]+[kK]?)\s*[-–~to]+\s*(\$?[\d,.]+[kK]?)', 'medium'),
        
        # Low confidence: General patterns with salary context
        (r'(?:[\w\s]*?salary[\w\s]*?:)?\s*(\$?[\d,.]+[kK]?)\s*[-–~to]+\s*(\$?[\d,.]+[kK]?)', 'low')
    ]

    # Process patterns in order
    for pattern, confidence in patterns:
        matches = re.finditer(pattern, text, re.I | re.DOTALL)
        for match in matches:
            min_amount = parse_amount(match.group(1))
            max_amount = parse_amount(match.group(2))
            
            if min_amount and max_amount:
                # Swap if min > max
                if min_amount > max_amount:
                    min_amount, max_amount = max_amount, min_amount
                    
                # Validate amounts
                if validate_salary(min_amount, text) and validate_salary(max_amount, text):
                    # Check ratio between min and max (shouldn't be too extreme)
                    if max_amount / min_amount <= 3.0:  # Max should not be more than 3x min
                        result['amount_min'] = min_amount
                        result['amount_max'] = max_amount
                        result['currency'] = find_currency(text)
                        result['interval'] = find_interval(text)
                        return result

    # Single number patterns (as fallback)
    single_patterns = [
        r'(?:salary|compensation|pay)\s*(?:is|:)?\s*(\$?[\d,.]+[kK]?)',
        r'(?:starting at|up to|from)\s*(\$?[\d,.]+[kK]?)'
    ]

    for pattern in single_patterns:
        match = re.search(pattern, text, re.I)
        if match:
            amount = parse_amount(match.group(1))
            if amount and validate_salary(amount, text):
                result['amount_min'] = amount
                result['amount_max'] = amount
                result['currency'] = find_currency(text)
                result['interval'] = find_interval(text)
                return result

    return result

def extract_structured_location(text: str) -> Dict:
    """Extract structured location information from text."""
    result = {
        'is_remote': False,
        'remote_type': None,  # fully, hybrid, optional
        'city': None,
        'state': None,
        'country': None,
        'raw_text': text
    }
    
    if not text:
        return result
        
    # Check for remote indicators
    remote_match = re.search(r'(remote|hybrid|wfh|work[- ]from[- ]home)', text, re.I)
    if remote_match:
        result['is_remote'] = True
        remote_type = remote_match.group(1).lower()
        if 'hybrid' in remote_type:
            result['remote_type'] = 'hybrid'
        elif any(x in remote_type for x in ['remote', 'wfh', 'work from home']):
            result['remote_type'] = 'fully'
            
    # Try to extract city, state (US)
    us_match = re.search(r'([A-Z][a-zA-Z\s]+),\s*([A-Z]{2})', text)
    if us_match:
        result['city'] = us_match.group(1).strip()
        result['state'] = us_match.group(2)
        result['country'] = 'US'
    else:
        # Try international format
        intl_match = re.search(r'([A-Z][a-zA-Z\s]+),\s*([A-Z][a-zA-Z\s]+)', text)
        if intl_match:
            result['city'] = intl_match.group(1).strip()
            result['country'] = intl_match.group(2).strip()
            
    return result

def extract_skills(text: str) -> Dict[str, List[str]]:
    """Extract technical skills from text."""
    result = {category: [] for category in TECH_SKILLS}
    
    if not text:
        return result
        
    # Convert text to lowercase for case-insensitive matching
    text_lower = text.lower()
    
    # Look for skills in each category
    for category, skills in TECH_SKILLS.items():
        for skill in skills:
            # Use word boundaries for more accurate matching
            if re.search(rf'\b{skill}\b', text_lower):
                result[category].append(skill)
                
    return result

def extract_seniority(text: str) -> Optional[str]:
    """Extract seniority level from text."""
    text_lower = text.lower()
    for pattern, level in SENIORITY_PATTERNS:
        if re.search(pattern, text_lower):
            return level
    return None

def normalize_employment_type(raw_type: Optional[str]) -> str:
    """Convert raw employment type string to enum value."""
    if not raw_type:
        return 'UNKNOWN'
        
    raw_type = raw_type.lower()
    
    if 'full' in raw_type and 'time' in raw_type:
        return 'FULL_TIME'
    elif 'part' in raw_type and 'time' in raw_type:
        return 'PART_TIME'
    elif 'contract' in raw_type:
        return 'CONTRACT'
    elif 'intern' in raw_type:
        return 'INTERNSHIP'
    elif 'temp' in raw_type:
        return 'TEMPORARY'
        
    return 'UNKNOWN'

def normalize_remote_status(raw_status: Optional[str]) -> str:
    """Convert raw remote status string to enum value."""
    if not raw_status:
        return 'UNKNOWN'
        
    raw_status = raw_status.lower()
    
    if 'remote' in raw_status:
        if 'hybrid' in raw_status:
            return 'HYBRID'
        return 'REMOTE'
    elif 'hybrid' in raw_status:
        return 'HYBRID'
    elif 'office' in raw_status or 'on-site' in raw_status or 'onsite' in raw_status:
        return 'OFFICE'
        
    return 'UNKNOWN'

def extract_with_patterns(text: str, patterns: List[str]) -> str:
    """Helper to extract text using a list of patterns."""
    for pattern in patterns:
        match = re.search(pattern, text, re.I)
        if match:
            return match.group(1)
    return "" 