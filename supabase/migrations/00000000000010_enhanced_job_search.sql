-- supabase/migrations/00000000000010_enhanced_job_search.sql
-- Drop existing functions first
DROP FUNCTION IF EXISTS test_search_jobs(text, text[], text, text, integer);
DROP FUNCTION IF EXISTS search_jobs(text, work_arrangement[], text, text, integer);

-- Create enhanced job search functionality
CREATE OR REPLACE FUNCTION search_jobs(
    search_text text DEFAULT NULL,
    work_types work_arrangement[] DEFAULT NULL,
    location_filter text DEFAULT NULL,
    remote_region text DEFAULT NULL,
    only_new boolean DEFAULT false,
    limit_val int DEFAULT 50
) RETURNS TABLE (
    id bigint,
    title text,
    company_name text,
    raw_location text,
    normalized_location text,
    department text,
    work_type work_arrangement,
    is_remote boolean,
    remote_region_info jsonb,
    posted_at timestamptz,
    search_rank real,
    is_new boolean,
    minutes_since_source_first_job integer
) AS $$
DECLARE
    processed_search_text text;
BEGIN
    -- Clean up search text for tsquery
    IF search_text IS NOT NULL AND search_text != '' THEN
        -- Replace special characters and convert spaces to &
        processed_search_text := regexp_replace(
            regexp_replace(search_text, '[-&|!(){}[\]^"~*?:\\]', ' ', 'g'),
            '\s+', ' & ', 'g'
        );
    END IF;

    RETURN QUERY
    SELECT 
        j.id,
        j.title::text,
        c.name::text as company_name,
        j.location::text as raw_location,
        j.normalized_location::text,
        j.department::text,
        j.work_arrangement_type as work_type,
        j.has_remote_indicator as is_remote,
        jsonb_build_object(
            'us_only', j.has_us_indicator,
            'uk_only', j.has_uk_indicator,
            'eu_only', j.has_eu_indicator
        ) as remote_region_info,
        j.created_at as posted_at,
        CASE 
            WHEN processed_search_text IS NULL THEN 1.0
            ELSE ts_rank(j.search_vector, to_tsquery('english', processed_search_text))
        END as search_rank,
        -- New job detection with explicit cast to boolean
        (EXTRACT(EPOCH FROM (j.first_seen - cs.oldest_job_seen))/60)::integer > 10 as is_new,
        -- Explicit cast to integer for minutes
        (EXTRACT(EPOCH FROM (j.first_seen - cs.oldest_job_seen))/60)::integer as minutes_since_source_first_job
    FROM jobs j
    JOIN companies c ON c.id = j.company_id
    JOIN company_sources cs ON cs.id = j.company_source_id
    WHERE 
        j.active = true
        AND (
            processed_search_text IS NULL 
            OR j.search_vector @@ to_tsquery('english', processed_search_text)
        )
        AND (
            work_types IS NULL 
            OR j.work_arrangement_type = ANY(work_types)
        )
        AND (
            location_filter IS NULL 
            OR j.normalized_location ~* location_filter
        )
        AND (
            remote_region IS NULL
            OR NOT j.has_remote_indicator
            OR (
                j.has_remote_indicator AND
                CASE remote_region
                    WHEN 'US' THEN j.has_us_indicator
                    WHEN 'UK' THEN j.has_uk_indicator
                    WHEN 'EU' THEN j.has_eu_indicator
                    ELSE true
                END
            )
        )
        AND (
            NOT only_new 
            OR (EXTRACT(EPOCH FROM (j.first_seen - cs.oldest_job_seen))/60)::integer > 10
        )
    ORDER BY 
        -- Modified ordering to boost new jobs
        CASE 
            WHEN location_filter IS NOT NULL 
                AND j.normalized_location ~* ('^' || location_filter) 
            THEN 2.0
            ELSE 1.0 
        END * search_rank 
        + CASE 
            WHEN (EXTRACT(EPOCH FROM (j.first_seen - cs.oldest_job_seen))/60)::integer > 10 THEN 0.5
            ELSE 0.0
          END DESC,
        j.first_seen DESC
    LIMIT limit_val;
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS test_search_jobs(text, text[], text, text, integer);

-- Create a simple test helper function
CREATE OR REPLACE FUNCTION test_search_jobs(
    search_text text DEFAULT NULL,
    work_types text[] DEFAULT NULL,
    location_filter text DEFAULT NULL,
    remote_region text DEFAULT NULL,
    only_new boolean DEFAULT false,
    limit_val int DEFAULT 5
) RETURNS TABLE (
    title text,
    company text,
    location text,
    work_type text,
    remote_info jsonb,
    is_new boolean,
    minutes_old integer
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        s.title,
        s.company_name,
        s.normalized_location,
        s.work_type::text,
        s.remote_region_info,
        s.is_new,
        s.minutes_since_source_first_job
    FROM search_jobs(
        search_text,
        CASE WHEN work_types IS NULL THEN NULL 
             ELSE work_types::work_arrangement[] END,
        location_filter,
        remote_region,
        only_new,
        limit_val
    ) s;
END;
$$ LANGUAGE plpgsql;

-- Add helpful comments
COMMENT ON FUNCTION search_jobs(text, work_arrangement[], text, text, boolean, integer) 
IS 'Enhanced job search function that supports:
- Full-text search across job title, department, and description
- Work arrangement filtering (remote/hybrid/onsite)
- Location filtering with normalized locations
- Remote region restrictions (US/UK/EU)
- New job detection and filtering
- Results ranked by search relevance, newness, and recency';

COMMENT ON FUNCTION test_search_jobs(text, text[], text, text, boolean, integer)
IS 'Helper function for testing job search with simplified output';

-- Example usage:
-- Find new jobs only:
-- SELECT * FROM test_search_jobs('python developer', NULL, NULL, NULL, true);
-- Find new remote US jobs:
-- SELECT * FROM test_search_jobs('developer', ARRAY['remote'], NULL, 'US', true);
-- Regular search with newness info:
-- SELECT * FROM test_search_jobs('senior engineer', NULL, 'new york');
-- Backward compatible search:
-- SELECT * FROM search_jobs('python', ARRAY['remote']::work_arrangement[], NULL, 'US', 50);
