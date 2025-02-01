-- supabase/migrations/00000000000009_new_job_detection.sql
-- Add single column to track oldest job
ALTER TABLE company_sources
    ADD COLUMN oldest_job_seen timestamptz;

-- Function to maintain oldest_job_seen
CREATE OR REPLACE FUNCTION update_oldest_job_seen()
RETURNS trigger AS $$
BEGIN
    -- Only for new active jobs
    IF (TG_OP = 'INSERT' AND NEW.active) THEN
        UPDATE company_sources
        SET oldest_job_seen = CASE 
            WHEN oldest_job_seen IS NULL THEN NEW.first_seen
            ELSE LEAST(oldest_job_seen, NEW.first_seen)
        END
        WHERE id = NEW.company_source_id;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Trigger to maintain oldest_job_seen
CREATE TRIGGER maintain_oldest_job_seen
    AFTER INSERT ON jobs
    FOR EACH ROW
    EXECUTE FUNCTION update_oldest_job_seen();

-- Initial population of oldest_job_seen
UPDATE company_sources cs
SET oldest_job_seen = (
    SELECT MIN(first_seen)
    FROM jobs
    WHERE company_source_id = cs.id
    AND active = true
);

-- Create rollback function
CREATE OR REPLACE FUNCTION rollback_new_job_detection() RETURNS void AS $$
BEGIN
    DROP TRIGGER IF EXISTS maintain_oldest_job_seen ON jobs;
    DROP FUNCTION IF EXISTS update_oldest_job_seen();
    ALTER TABLE company_sources DROP COLUMN IF EXISTS oldest_job_seen;
END;
$$ LANGUAGE plpgsql;