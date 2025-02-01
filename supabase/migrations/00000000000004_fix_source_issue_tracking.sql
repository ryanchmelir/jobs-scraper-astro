-- supabase/migrations/00000000000004_fix_source_issue_tracking.sql
-- This migration is dependent on the 00000000000003_fix_ambiguous_id.sql migration
-- Fix the track_source_issue function to properly handle upserts
DROP FUNCTION IF EXISTS track_source_issue(bigint, text);
CREATE OR REPLACE FUNCTION track_source_issue(
    source_id bigint,
    error_message text
) RETURNS void AS $$
DECLARE
    existing_record RECORD;
BEGIN
    -- First try to update existing record
    UPDATE company_source_issues 
    SET 
        failure_count = COALESCE(failure_count, 0) + 1,
        last_failure = NOW(),
        last_error = error_message
    WHERE company_source_id = source_id;
    
    -- If no record was updated, insert new one
    IF NOT FOUND THEN
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
            failure_count = COALESCE(company_source_issues.failure_count, 0) + 1,
            last_failure = NOW(),
            last_error = error_message;
    END IF;

    -- Update the next_scrape_time based on failure count
    UPDATE company_sources
    SET next_scrape_time = 
        CASE 
            WHEN (SELECT failure_count FROM company_source_issues WHERE company_source_id = source_id) >= 3 
            THEN NOW() + INTERVAL '1 day'
            ELSE NOW() + INTERVAL '10 minutes'
        END
    WHERE id = source_id;
END;
$$ LANGUAGE plpgsql; 