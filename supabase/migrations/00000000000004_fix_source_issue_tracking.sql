-- supabase/migrations/00000000000004_fix_source_issue_tracking.sql
-- This migration is dependent on the 00000000000003_fix_ambiguous_id.sql migration
-- Fix the track_source_issue function to properly handle upserts
DROP FUNCTION IF EXISTS track_source_issue(bigint, text);
CREATE OR REPLACE FUNCTION track_source_issue(
    in_source_id bigint,
    in_error_message text
) RETURNS void AS $$
DECLARE
    existing_record RECORD;
    current_failures integer;
BEGIN
    -- First try to update existing record
    UPDATE company_source_issues 
    SET 
        failure_count = COALESCE(failure_count, 0) + 1,
        last_failure = NOW(),
        last_error = in_error_message
    WHERE company_source_id = in_source_id;
    
    -- If no record was updated, insert new one
    IF NOT FOUND THEN
        INSERT INTO company_source_issues (
            company_source_id,
            failure_count,
            last_failure,
            last_error
        )
        VALUES (
            in_source_id,
            1,
            NOW(),
            in_error_message
        )
        ON CONFLICT (company_source_id) DO UPDATE
        SET 
            failure_count = COALESCE(company_source_issues.failure_count, 0) + 1,
            last_failure = NOW(),
            last_error = in_error_message;
    END IF;

    -- Get current failure count
    SELECT failure_count INTO current_failures
    FROM company_source_issues 
    WHERE company_source_id = in_source_id;

    -- Update the next_scrape_time based on failure count
    UPDATE company_sources
    SET next_scrape_time = 
        CASE 
            WHEN current_failures >= 3 THEN NOW() + INTERVAL '1 day'
            ELSE NOW() + INTERVAL '10 minutes'
        END
    WHERE id = in_source_id;
END;
$$ LANGUAGE plpgsql; 