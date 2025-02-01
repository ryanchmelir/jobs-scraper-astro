-- Fix the update_source_scrape_time function to handle ambiguous column reference
DROP FUNCTION IF EXISTS update_source_scrape_time(bigint, integer);
CREATE OR REPLACE FUNCTION update_source_scrape_time(
    in_source_id bigint,
    in_next_interval integer
) RETURNS void AS $$
BEGIN
    UPDATE company_sources 
    SET last_scraped = NOW(),
        next_scrape_time = NOW() + (in_next_interval || ' minutes')::interval
    WHERE id = in_source_id;
END;
$$ LANGUAGE plpgsql; 