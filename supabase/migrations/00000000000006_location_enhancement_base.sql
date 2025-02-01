-- 00000000000006_location_enhancement_base.sql
-- Enable required extensions if not already enabled
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- Create types if they don't exist
DO $$ 
BEGIN
    -- Create work_arrangement type if it doesn't exist
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'work_arrangement') THEN
        CREATE TYPE work_arrangement AS ENUM ('remote', 'hybrid', 'onsite');
    END IF;

    -- Create location_resolution_status type if it doesn't exist
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'location_resolution_status') THEN
        CREATE TYPE location_resolution_status AS ENUM ('pending', 'processing', 'completed', 'failed');
    END IF;
END
$$;

-- Enhance jobs table
ALTER TABLE jobs
    ADD COLUMN normalized_location text
    GENERATED ALWAYS AS (
        trim(both ' ' from regexp_replace(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            lower(location),
                            '^(remote|hybrid|onsite|virtual|anywhere)(\s*[,-])?\s*', 
                            ''
                        ),
                        '\s*\([^)]*\)\s*',  -- Remove parenthetical expressions
                        ''
                    ),
                    '\s*,\s*',  -- Normalize commas
                    ', '
                ),
                '\s+',  -- Normalize spaces
                ' '
            ),
            '^\s+|\s+$',  -- Trim spaces
            ''
        ))
    ) STORED,
    ADD COLUMN work_arrangement_type work_arrangement
    GENERATED ALWAYS AS (
        CASE 
            WHEN position(' remote ' in ' ' || lower(location) || ' ') > 0 
                OR position(' remote,' in ' ' || lower(location) || ' ') > 0
                OR position('remote ' in lower(location)) = 1
                OR position('remote,' in lower(location)) = 1
                OR position('(remote)' in lower(location)) > 0
                OR position('( remote )' in lower(location)) > 0
                OR position('(remote ' in lower(location)) > 0
                OR position(' remote)' in lower(location)) > 0
                OR position(' virtual ' in ' ' || lower(location) || ' ') > 0 
                OR position(' virtual,' in ' ' || lower(location) || ' ') > 0
                OR position('virtual ' in lower(location)) = 1
                OR position('virtual,' in lower(location)) = 1
                OR position('(virtual)' in lower(location)) > 0
                OR position(' anywhere ' in ' ' || lower(location) || ' ') > 0 
                OR position(' anywhere,' in ' ' || lower(location) || ' ') > 0
                OR position('anywhere ' in lower(location)) = 1
                OR position('anywhere,' in lower(location)) = 1
                OR position('(anywhere)' in lower(location)) > 0
                THEN 'remote'::work_arrangement
            WHEN position(' hybrid ' in ' ' || lower(location) || ' ') > 0 
                OR position(' hybrid,' in ' ' || lower(location) || ' ') > 0
                OR position('hybrid ' in lower(location)) = 1
                OR position('hybrid,' in lower(location)) = 1
                THEN 'hybrid'::work_arrangement
            ELSE 'onsite'::work_arrangement
        END
    ) STORED,
    ADD COLUMN has_remote_indicator boolean
    GENERATED ALWAYS AS (
        position(' remote ' in ' ' || lower(location) || ' ') > 0 
        OR position(' remote,' in ' ' || lower(location) || ' ') > 0
        OR position('remote ' in lower(location)) = 1
        OR position('remote,' in lower(location)) = 1
        OR position('(remote)' in lower(location)) > 0
        OR position('( remote )' in lower(location)) > 0
        OR position('(remote ' in lower(location)) > 0
        OR position(' remote)' in lower(location)) > 0
        OR position(' virtual ' in ' ' || lower(location) || ' ') > 0 
        OR position(' virtual,' in ' ' || lower(location) || ' ') > 0
        OR position('virtual ' in lower(location)) = 1
        OR position('virtual,' in lower(location)) = 1
        OR position('(virtual)' in lower(location)) > 0
        OR position(' anywhere ' in ' ' || lower(location) || ' ') > 0 
        OR position(' anywhere,' in ' ' || lower(location) || ' ') > 0
        OR position('anywhere ' in lower(location)) = 1
        OR position('anywhere,' in lower(location)) = 1
        OR position('(anywhere)' in lower(location)) > 0
    ) STORED,
    ADD COLUMN has_hybrid_indicator boolean
    GENERATED ALWAYS AS (
        position(' hybrid ' in ' ' || lower(location) || ' ') > 0
        OR position(' hybrid,' in ' ' || lower(location) || ' ') > 0
        OR position('hybrid ' in lower(location)) = 1
        OR position('hybrid,' in lower(location)) = 1
    ) STORED,
    ADD COLUMN has_us_indicator boolean
    GENERATED ALWAYS AS (
        position(' usa ' in ' ' || lower(location) || ' ') > 0 
        OR position(' usa,' in ' ' || lower(location) || ' ') > 0
        OR position('usa ' in lower(location)) = 1
        OR position('usa,' in lower(location)) = 1
        OR position('(usa)' in lower(location)) > 0
        OR position('(usa ' in lower(location)) > 0
        OR position(' usa)' in lower(location)) > 0
        OR position(' us ' in ' ' || lower(location) || ' ') > 0 
        OR position(' us,' in ' ' || lower(location) || ' ') > 0
        OR position('us ' in lower(location)) = 1
        OR position('us,' in lower(location)) = 1
        OR position('(us)' in lower(location)) > 0
        OR position('(us ' in lower(location)) > 0
        OR position(' us)' in lower(location)) > 0
        OR position('(us-' in lower(location)) > 0
        OR position('(us ' in lower(location)) > 0
        OR position('united states' in lower(location)) > 0
    ) STORED,
    ADD COLUMN has_uk_indicator boolean
    GENERATED ALWAYS AS (
        position(' uk ' in ' ' || lower(location) || ' ') > 0 
        OR position(' uk,' in ' ' || lower(location) || ' ') > 0 
        OR position('uk ' in lower(location)) = 1
        OR position('uk,' in lower(location)) = 1
        OR position('(uk)' in lower(location)) > 0
        OR position('(uk ' in lower(location)) > 0
        OR position(' uk)' in lower(location)) > 0
        OR position('(uk-' in lower(location)) > 0
        OR position('united kingdom' in lower(location)) > 0
        OR position('great britain' in lower(location)) > 0
    ) STORED,
    ADD COLUMN has_eu_indicator boolean
    GENERATED ALWAYS AS (
        position(' eu ' in ' ' || lower(location) || ' ') > 0 
        OR position(' eu,' in ' ' || lower(location) || ' ') > 0 
        OR position('eu ' in lower(location)) = 1
        OR position('eu,' in lower(location)) = 1
        OR position('(eu)' in lower(location)) > 0
        OR position('(eu ' in lower(location)) > 0
        OR position(' eu)' in lower(location)) > 0
        OR position('(eu-' in lower(location)) > 0
        OR position('european union' in lower(location)) > 0
        OR position('europe' in lower(location)) > 0
    ) STORED,
    ADD COLUMN primary_geonameid integer REFERENCES geonames.locations(geonameid),
    ADD COLUMN secondary_geonames integer[],
    ADD COLUMN location_confidence decimal,
    ADD COLUMN location_analysis jsonb;

-- Create indexes
CREATE INDEX idx_jobs_work_arrangement ON jobs (work_arrangement_type);
CREATE INDEX idx_jobs_normalized_location ON jobs (normalized_location);
CREATE INDEX idx_jobs_location_indicators ON jobs (
    has_remote_indicator,
    has_hybrid_indicator,
    has_us_indicator,
    has_uk_indicator,
    has_eu_indicator
);
CREATE INDEX idx_jobs_primary_geo ON jobs (primary_geonameid);
CREATE INDEX idx_jobs_location_confidence ON jobs (location_confidence) WHERE location_confidence IS NOT NULL;

-- Rollback SQL
CREATE OR REPLACE FUNCTION rollback_location_enhancement_base() RETURNS void AS $$
BEGIN
    DROP INDEX IF EXISTS idx_jobs_work_arrangement;
    DROP INDEX IF EXISTS idx_jobs_normalized_location;
    DROP INDEX IF EXISTS idx_jobs_location_indicators;
    DROP INDEX IF EXISTS idx_jobs_primary_geo;
    DROP INDEX IF EXISTS idx_jobs_location_confidence;
    
    ALTER TABLE jobs
        DROP COLUMN IF EXISTS work_arrangement_type,
        DROP COLUMN IF EXISTS normalized_location,
        DROP COLUMN IF EXISTS has_remote_indicator,
        DROP COLUMN IF EXISTS has_hybrid_indicator,
        DROP COLUMN IF EXISTS has_us_indicator,
        DROP COLUMN IF EXISTS has_uk_indicator,
        DROP COLUMN IF EXISTS has_eu_indicator,
        DROP COLUMN IF EXISTS has_bay_area_indicator,
        DROP COLUMN IF EXISTS has_metro_indicator,
        DROP COLUMN IF EXISTS primary_geonameid,
        DROP COLUMN IF EXISTS secondary_geonames,
        DROP COLUMN IF EXISTS location_confidence,
        DROP COLUMN IF EXISTS location_analysis;
    
    DROP TYPE IF EXISTS work_arrangement;
    DROP TYPE IF EXISTS location_resolution_status;
END;
$$ LANGUAGE plpgsql;