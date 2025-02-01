/*
-- supabase/migrations/00000000000007_location_resolution_system.sql
-- Create queue table
CREATE TABLE location_resolution_queue (
    job_id bigint PRIMARY KEY REFERENCES jobs(id),
    status location_resolution_status NOT NULL DEFAULT 'pending',
    attempts integer DEFAULT 0,
    last_attempt timestamp with time zone,
    last_error text,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);

-- Create indexes for queue
CREATE INDEX idx_location_queue_status ON location_resolution_queue (status);
CREATE INDEX idx_location_queue_created ON location_resolution_queue (created_at) WHERE status = 'pending';

-- Create updated_at trigger
CREATE TRIGGER update_location_queue_timestamp
    BEFORE UPDATE ON location_resolution_queue
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Create location resolution function
CREATE OR REPLACE FUNCTION resolve_job_location(target_job_id bigint)
RETURNS void AS $$
DECLARE
    job_location text;
    location_parts text[];
    primary_location text;
    state_code text;
    matching_location record;
    country_hint text;
    city_name text;
BEGIN
    -- Get job location details
    SELECT normalized_location
    INTO job_location
    FROM jobs 
    WHERE id = target_job_id;

    -- Split on semicolons first to get individual city listings
    location_parts := string_to_array(job_location, ';');
    
    -- Take the first location and split it into city and state
    primary_location := trim(both ' ' from location_parts[1]);
    
    -- Extract state code if present (assuming format "City, ST")
    state_code := upper(trim(both ' ' from split_part(split_part(primary_location, ',', 2), ';', 1)));
    
    -- Set country hint based on state code or existing indicators
    SELECT 
        CASE 
            WHEN state_code ~ '^(AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY)$' THEN 'US'
            WHEN has_us_indicator THEN 'US'
            WHEN has_uk_indicator THEN 'GB'
            WHEN has_eu_indicator THEN 'EU'
            ELSE NULL
        END
    INTO country_hint
    FROM jobs
    WHERE id = target_job_id;

    -- Update queue status
    UPDATE location_resolution_queue 
    SET status = 'processing',
        attempts = attempts + 1,
        last_attempt = now()
    WHERE job_id = target_job_id;

    -- Prepare city name once
    city_name := trim(split_part(primary_location, ',', 1));

    -- Try exact match with city name using CTE
    WITH candidates AS (
        SELECT 
            l.geonameid,
            l.name,
            l.population,
            -- Compute base name match score
            GREATEST(
                CASE WHEN l.name ILIKE city_name THEN 1.0
                     WHEN l.name % city_name THEN 0.9
                     ELSE similarity(l.name, city_name)
                END,
                CASE WHEN EXISTS (
                    SELECT 1 
                    FROM geonames.alternate_names an 
                    WHERE an.geonameid = l.geonameid 
                    AND (an.alternate_name ILIKE city_name OR an.alternate_name % city_name)
                    LIMIT 1
                ) THEN 0.9
                ELSE 0.0
                END
            ) * 
            -- Location type score
            CASE
                WHEN state_code IS NOT NULL AND l.admin1_code = state_code THEN 1.5
                WHEN country_hint IS NOT NULL AND l.country_code = country_hint THEN 1.2
                ELSE 0.8
            END *
            -- Population score
            CASE
                WHEN l.population > 1000000 THEN 1.3
                WHEN l.population > 100000 THEN 1.2
                WHEN l.feature_class = 'P' THEN 1.0
                ELSE 0.7
            END as match_confidence
        FROM geonames.locations l
        WHERE l.feature_class = 'P'  -- Only match populated places
        AND (
            -- Use trigram index for initial filtering
            l.name % city_name
            OR EXISTS (
                SELECT 1 
                FROM geonames.alternate_names an 
                WHERE an.geonameid = l.geonameid 
                AND an.alternate_name % city_name
                LIMIT 1
            )
        )
        AND (
            -- Location type filtering
            (state_code IS NOT NULL AND l.admin1_code = state_code)
            OR (country_hint IS NOT NULL AND l.country_code = country_hint)
            OR (country_hint = 'EU' AND l.country_code = ANY(ARRAY[
                'AT', 'BE', 'BG', 'HR', 'CY', 'CZ', 'DK', 'EE', 'FI', 'FR', 
                'DE', 'GR', 'HU', 'IE', 'IT', 'LV', 'LT', 'LU', 'MT', 'NL', 
                'PL', 'PT', 'RO', 'SK', 'SI', 'ES', 'SE'
            ]))
        )
    )
    SELECT *
    INTO matching_location
    FROM candidates
    ORDER BY match_confidence DESC, population DESC NULLS LAST
    LIMIT 1;

    -- Update job with results
    UPDATE jobs
    SET 
        primary_geonameid = matching_location.geonameid,
        location_confidence = COALESCE(matching_location.match_confidence, 0),
        location_analysis = jsonb_build_object(
            'match_type', CASE 
                WHEN matching_location.match_confidence >= 1.2 THEN 'exact'
                WHEN matching_location.match_confidence >= 0.8 THEN 'fuzzy'
                ELSE 'none'
            END,
            'confidence', matching_location.match_confidence,
            'resolution_time', now(),
            'location_parts', location_parts,
            'country_hint', country_hint,
            'state_code', state_code,
            'primary_location', primary_location
        )
    WHERE id = target_job_id;

    -- Update queue status
    UPDATE location_resolution_queue 
    SET status = 'completed'
    WHERE job_id = target_job_id;

EXCEPTION WHEN OTHERS THEN
    -- Log error and update queue
    UPDATE location_resolution_queue 
    SET 
        status = 'failed',
        last_error = SQLERRM
    WHERE job_id = target_job_id;
END;
$$ LANGUAGE plpgsql;

-- Create trigger for queueing new jobs
CREATE OR REPLACE FUNCTION queue_location_resolution()
RETURNS trigger AS $$
BEGIN
    INSERT INTO location_resolution_queue (job_id)
    VALUES (NEW.id);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER queue_job_location_resolution
AFTER INSERT ON jobs
FOR EACH ROW
EXECUTE FUNCTION queue_location_resolution();

-- Create view for enhanced job search
CREATE VIEW enhanced_job_search AS
SELECT 
    j.*,
    g.name as geoname_normalized_location,
    g.latitude,
    g.longitude,
    g.country_code,
    g.admin1_code,
    g.feature_class,
    g.population
FROM jobs j
LEFT JOIN geonames.locations g ON j.primary_geonameid = g.geonameid
WHERE j.active = true;

-- Add to migration 007
CREATE INDEX IF NOT EXISTS idx_geonames_name_trgm 
ON geonames.locations USING gin (name gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_geonames_feature_country 
ON geonames.locations (feature_class, country_code, population DESC);

CREATE INDEX IF NOT EXISTS idx_alternate_names_trgm 
ON geonames.alternate_names USING gin (alternate_name gin_trgm_ops);

-- Rollback SQL
CREATE OR REPLACE FUNCTION rollback_location_resolution_system() RETURNS void AS $$
BEGIN
    DROP VIEW IF EXISTS enhanced_job_search;
    DROP TRIGGER IF EXISTS queue_job_location_resolution ON jobs;
    DROP FUNCTION IF EXISTS queue_location_resolution();
    DROP FUNCTION IF EXISTS resolve_job_location(bigint);
    DROP TRIGGER IF EXISTS update_location_queue_timestamp ON location_resolution_queue;
    DROP TABLE IF EXISTS location_resolution_queue;
END;
$$ LANGUAGE plpgsql;
*/