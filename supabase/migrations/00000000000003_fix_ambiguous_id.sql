-- Fix ambiguous id in upsert_jobs_batch function
-- Depends on: 00000000000003_enhanced_job_search.sql

CREATE OR REPLACE FUNCTION upsert_jobs_batch(jobs_data jsonb)
RETURNS TABLE (id bigint) AS $$
BEGIN
    RETURN QUERY
    INSERT INTO jobs (
        company_id,
        company_source_id,
        source_job_id,
        title,
        location,
        department,
        url,
        raw_data,
        active,
        first_seen,
        last_seen,
        created_at,
        updated_at,
        needs_details
    )
    SELECT 
        cs.company_id,
        (jobs_data->>'company_source_id')::bigint,
        jobs_data->>'source_job_id',
        jobs_data->>'title',
        jobs_data->>'location',
        jobs_data->>'department',
        jobs_data->>'url',
        (jobs_data->>'raw_data')::jsonb,
        true,
        NOW(),
        NOW(),
        NOW(),
        NOW(),
        false
    FROM company_sources cs
    WHERE cs.id = (jobs_data->>'company_source_id')::bigint
    ON CONFLICT (company_source_id, source_job_id) DO UPDATE
    SET 
        title = EXCLUDED.title,
        location = EXCLUDED.location,
        department = EXCLUDED.department,
        url = EXCLUDED.url,
        raw_data = EXCLUDED.raw_data,
        active = true,
        last_seen = NOW(),
        updated_at = NOW(),
        needs_details = false
    RETURNING jobs.id;
END;
$$ LANGUAGE plpgsql; 

DROP FUNCTION IF EXISTS update_source_config(bigint, text);
CREATE OR REPLACE FUNCTION update_source_config(in_source_id bigint, in_url_pattern text)
RETURNS void AS $$
BEGIN
    UPDATE company_sources 
    SET config = json_build_object(
        'working_url_pattern', 
        COALESCE(in_url_pattern, config->>'working_url_pattern')
    ),
    next_scrape_time = NOW() + INTERVAL '10 minutes'
    WHERE id = in_source_id;
END;
$$ LANGUAGE plpgsql; 

DROP FUNCTION IF EXISTS track_source_issue(bigint, text);
-- Fix track_source_issue
CREATE OR REPLACE FUNCTION track_source_issue(in_source_id bigint, in_error text)
RETURNS void AS $$
BEGIN
    INSERT INTO company_source_issues (company_source_id, error)
    VALUES (in_source_id, in_error);
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS mark_jobs_inactive(bigint);
-- Fix mark_jobs_inactive
CREATE OR REPLACE FUNCTION mark_jobs_inactive(in_source_id bigint)
RETURNS void AS $$
BEGIN
    UPDATE jobs
    SET active = false,
        updated_at = NOW()
    WHERE company_source_id = in_source_id
    AND active = true;
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS clear_source_issues(bigint);
-- Fix clear_source_issues
CREATE OR REPLACE FUNCTION clear_source_issues(in_source_id bigint)
RETURNS void AS $$
BEGIN
    DELETE FROM company_source_issues
    WHERE company_source_id = in_source_id;
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS update_source_scrape_time(bigint);
-- Fix update_source_scrape_time
CREATE OR REPLACE FUNCTION update_source_scrape_time(in_source_id bigint)
RETURNS void AS $$
BEGIN
    UPDATE company_sources
    SET last_scrape_time = NOW(),
        next_scrape_time = NOW() + INTERVAL '1 day'
    WHERE id = in_source_id;
END;
$$ LANGUAGE plpgsql; 