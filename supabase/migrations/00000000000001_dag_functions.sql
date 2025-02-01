-- supabase/migrations/00000000000001_dag_functions.sql

-- Function to get sources due for scraping
CREATE OR REPLACE FUNCTION get_sources_for_scraping(
    batch_size integer DEFAULT 10
) RETURNS TABLE (
    id bigint,
    company_id bigint,
    source_type source_type,
    source_id varchar,
    config jsonb,
    failure_count integer
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        cs.id,
        cs.company_id,
        cs.source_type,
        cs.source_id,
        cs.config,
        COALESCE(csi.failure_count, 0)
    FROM company_sources cs
    LEFT JOIN company_source_issues csi ON cs.id = csi.company_source_id
    WHERE cs.active = true 
    AND (cs.next_scrape_time <= NOW() OR cs.next_scrape_time IS NULL)
    AND (csi.failure_count IS NULL OR csi.failure_count < 4)
    ORDER BY cs.next_scrape_time ASC NULLS FIRST
    LIMIT batch_size
    FOR UPDATE OF cs SKIP LOCKED;
END;
$$ LANGUAGE plpgsql;

-- Function to track source issues
CREATE OR REPLACE FUNCTION track_source_issue(
    source_id bigint,
    error_message text
) RETURNS void AS $$
BEGIN
    INSERT INTO company_source_issues (
        company_source_id,
        failure_count,
        last_failure,
        last_error
    )
    VALUES (
        source_id,
        1,
        NOW(),
        error_message
    )
    ON CONFLICT (company_source_id) DO UPDATE
    SET 
        failure_count = company_source_issues.failure_count + 1,
        last_failure = NOW(),
        last_error = error_message;
END;
$$ LANGUAGE plpgsql;

-- Function to upsert jobs in batch
CREATE OR REPLACE FUNCTION upsert_jobs_batch(
    jobs_data jsonb
) RETURNS TABLE (id bigint) AS $$
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
    RETURNING id;
END;
$$ LANGUAGE plpgsql;

-- Function to mark jobs as inactive
CREATE OR REPLACE FUNCTION mark_jobs_inactive(
    p_company_source_id bigint,
    p_job_ids text[]
) RETURNS void AS $$
BEGIN
    UPDATE jobs 
    SET 
        active = false,
        updated_at = NOW()
    WHERE 
        company_source_id = p_company_source_id
        AND source_job_id = ANY(p_job_ids);
END;
$$ LANGUAGE plpgsql;