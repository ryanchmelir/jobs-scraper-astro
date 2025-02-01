/*
-- supabase/migrations/00000000000008_location_processing.sql
-- Enable pg_cron extension
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS cron;

-- Create parallel processing function
CREATE OR REPLACE FUNCTION process_location_resolution_queue_parallel(
    worker_id int,
    num_workers int,
    batch_size int DEFAULT 25
)
RETURNS void AS $$
DECLARE
    job record;
    start_time timestamp;
BEGIN
    start_time := clock_timestamp();
    
    FOR job IN 
        SELECT job_id 
        FROM location_resolution_queue 
        WHERE status = 'pending'
        AND (last_attempt IS NULL OR last_attempt < now() - interval '5 minutes')
        AND MOD(job_id, num_workers) = worker_id  -- Distribute jobs across workers
        ORDER BY created_at 
        LIMIT batch_size
        FOR UPDATE SKIP LOCKED
    LOOP
        PERFORM resolve_job_location(job.job_id);
    END LOOP;

    -- Log processing time if it's taking too long
    IF clock_timestamp() - start_time > interval '45 seconds' THEN
        RAISE NOTICE 'Worker % took % to process batch', worker_id, clock_timestamp() - start_time;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Create retry function
CREATE OR REPLACE FUNCTION retry_failed_location_resolutions()
RETURNS void AS $$
BEGIN
    UPDATE location_resolution_queue 
    SET status = 'pending',
        attempts = attempts + 1 
    WHERE status = 'failed'
    AND attempts < 3 
    AND last_attempt < now() - interval '15 minutes';
END;
$$ LANGUAGE plpgsql;

-- Schedule processing (wrapped in DO block for idempotency)
DO $$
BEGIN
    -- Remove any existing schedules
    BEGIN
        PERFORM cron.unschedule('process-location-queue');
    EXCEPTION 
        WHEN OTHERS THEN 
            NULL;  -- Ignore if it doesn't exist
    END;
    
    BEGIN
        PERFORM cron.unschedule('retry-failed-locations');
    EXCEPTION 
        WHEN OTHERS THEN 
            NULL;  -- Ignore if it doesn't exist
    END;

    -- Remove any existing parallel worker schedules
    FOR i IN 0..3 LOOP
        BEGIN
            PERFORM cron.unschedule('process-location-queue-' || i);
        EXCEPTION 
            WHEN OTHERS THEN 
                NULL;  -- Ignore if it doesn't exist
        END;
    END LOOP;
    
    -- Schedule parallel workers
    FOR i IN 0..3 LOOP
        PERFORM cron.schedule(
            'process-location-queue-' || i,
            '* * * * *',  -- Every minute
            format('SELECT process_location_resolution_queue_parallel(%s, 4, 25)', i)
        );
    END LOOP;

    -- Schedule retry job
    PERFORM cron.schedule(
        'retry-failed-locations',
        '*/15 * * * *',  -- Every 15 minutes
        'SELECT retry_failed_location_resolutions()'
    );
END;
$$;

-- Rollback function
CREATE OR REPLACE FUNCTION rollback_location_processing() RETURNS void AS $$
BEGIN
    -- Unschedule all workers
    FOR i IN 0..3 LOOP
        BEGIN
            PERFORM cron.unschedule('process-location-queue-' || i);
        EXCEPTION 
            WHEN OTHERS THEN 
                NULL;  -- Ignore if it doesn't exist
        END;
    END LOOP;

    -- Unschedule retry job
    BEGIN
        PERFORM cron.unschedule('retry-failed-locations');
    EXCEPTION 
        WHEN OTHERS THEN 
            NULL;  -- Ignore if it doesn't exist
    END;
    
    -- Drop functions
    DROP FUNCTION IF EXISTS process_location_resolution_queue(int);
    DROP FUNCTION IF EXISTS process_location_resolution_queue_parallel(int, int, int);
    DROP FUNCTION IF EXISTS retry_failed_location_resolutions();
END;
$$ LANGUAGE plpgsql;
*/