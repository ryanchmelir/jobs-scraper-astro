-- supabase/migrations/00000000000011_landing_page.sql
-- Recent jobs function
DROP FUNCTION get_recent_jobs(integer,integer);
CREATE OR REPLACE FUNCTION get_recent_jobs(
    hours_ago integer DEFAULT 1,
    limit_val integer DEFAULT 10
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
    first_seen timestamptz,
    is_new boolean,
    minutes_since_source_first_job integer,
    url text
) SECURITY DEFINER
SET search_path = public
AS $$
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
        j.first_seen,
        (EXTRACT(EPOCH FROM (j.first_seen - cs.oldest_job_seen))/60)::integer > 10 as is_new,
        (EXTRACT(EPOCH FROM (j.first_seen - cs.oldest_job_seen))/60)::integer as minutes_since_source_first_job,
        j.url
    FROM jobs j
    JOIN companies c ON c.id = j.company_id
    JOIN company_sources cs ON cs.id = j.company_source_id
    WHERE 
        j.active = true
        AND j.first_seen >= NOW() - (hours_ago || ' hours')::interval
    ORDER BY j.first_seen DESC
    LIMIT limit_val;
$$ LANGUAGE SQL;

-- Stats function
CREATE OR REPLACE FUNCTION get_source_stats()
RETURNS jsonb SECURITY DEFINER
SET search_path = public
AS $$
    SELECT jsonb_build_object(
        'total_active_jobs', (SELECT COUNT(*) FROM jobs WHERE active = true),
        'active_sources', (SELECT COUNT(DISTINCT id) FROM companies WHERE active = true),
        'newest_job_age', (
            SELECT EXTRACT(EPOCH FROM (NOW() - MAX(last_scraped_at)))/60 
            FROM company_sources
        )
    );
$$ LANGUAGE SQL;