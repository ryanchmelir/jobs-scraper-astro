-- Additional functions for job discovery DAG
-- Depends on: 00000000000001_dag_functions.sql

-- Function to update source config and next scrape time
CREATE OR REPLACE FUNCTION update_source_config(
    source_id bigint,
    url_pattern text
) RETURNS void AS $$
BEGIN
    UPDATE company_sources 
    SET config = json_build_object(
        'working_url_pattern', 
        COALESCE(url_pattern, config->>'working_url_pattern')
    ),
    next_scrape_time = NOW() + INTERVAL '10 minutes'
    WHERE id = source_id;
END;
$$ LANGUAGE plpgsql;

-- Function to clear source issues
CREATE OR REPLACE FUNCTION clear_source_issues(
    source_id bigint
) RETURNS void AS $$
BEGIN
    DELETE FROM company_source_issues
    WHERE company_source_id = source_id;
END;
$$ LANGUAGE plpgsql;

-- Function to update source scrape time
CREATE OR REPLACE FUNCTION update_source_scrape_time(
    source_id bigint,
    next_interval integer
) RETURNS void AS $$
BEGIN
    UPDATE company_sources 
    SET last_scraped = NOW(),
        next_scrape_time = NOW() + (next_interval || ' minutes')::interval
    WHERE id = source_id;
END;
$$ LANGUAGE plpgsql; 