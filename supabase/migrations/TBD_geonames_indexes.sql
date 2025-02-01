-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Add indexes for common queries
CREATE INDEX IF NOT EXISTS idx_locations_country 
ON geonames.locations(country_code);

CREATE INDEX IF NOT EXISTS idx_locations_admin1 
ON geonames.locations(country_code, admin1_code);

CREATE INDEX IF NOT EXISTS idx_locations_admin2 
ON geonames.locations(country_code, admin1_code, admin2_code);

-- Full text search on names
CREATE INDEX IF NOT EXISTS idx_locations_name_trgm 
ON geonames.locations USING gin (name gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_locations_ascii_name_trgm 
ON geonames.locations USING gin (ascii_name gin_trgm_ops);

-- Population and timezone queries
CREATE INDEX IF NOT EXISTS idx_locations_population 
ON geonames.locations(population DESC NULLS LAST);

CREATE INDEX IF NOT EXISTS idx_locations_timezone 
ON geonames.locations(timezone);

-- Helper function to get location hierarchy
CREATE OR REPLACE FUNCTION geonames.get_location_hierarchy(
    p_geoname_id BIGINT
) RETURNS TABLE (
    level INT,
    geoname_id BIGINT,
    name TEXT,
    feature_class CHAR(1),
    feature_code TEXT
) AS $$
WITH RECURSIVE hierarchy AS (
    -- Base case: start with the given location
    SELECT 
        0 as level,
        l.geoname_id,
        l.name,
        l.feature_class,
        l.feature_code,
        l.country_code,
        l.admin1_code,
        l.admin2_code
    FROM geonames.locations l
    WHERE l.geoname_id = p_geoname_id

    UNION ALL

    -- Recursive case: find parent locations
    SELECT 
        h.level + 1,
        l.geoname_id,
        l.name,
        l.feature_class,
        l.feature_code,
        l.country_code,
        l.admin1_code,
        l.admin2_code
    FROM hierarchy h
    JOIN geonames.locations l ON (
        l.country_code = h.country_code AND
        (
            -- Country level
            (h.admin1_code IS NULL AND l.feature_code = 'PCLI') OR
            -- Admin1 level
            (h.admin1_code = l.admin1_code AND l.feature_class = 'A' AND l.feature_code = 'ADM1') OR
            -- Admin2 level
            (h.admin2_code = l.admin2_code AND l.feature_class = 'A' AND l.feature_code = 'ADM2')
        )
    )
    WHERE h.level < 3  -- Limit to 3 levels up
)
SELECT level, geoname_id, name, feature_class, feature_code
FROM hierarchy
ORDER BY level DESC;
$$ LANGUAGE SQL STABLE;

-- Helper function to search locations
CREATE OR REPLACE FUNCTION geonames.search_locations(
    search_text TEXT,
    country_code TEXT DEFAULT NULL,
    feature_class CHAR(1) DEFAULT NULL,
    min_population BIGINT DEFAULT NULL,
    limit_count INT DEFAULT 10
) RETURNS TABLE (
    geoname_id BIGINT,
    name TEXT,
    ascii_name TEXT,
    country_code CHAR(2),
    admin1_code TEXT,
    admin2_code TEXT,
    feature_class CHAR(1),
    feature_code TEXT,
    population BIGINT,
    timezone TEXT,
    similarity FLOAT
) AS $$
SELECT 
    l.geoname_id,
    l.name,
    l.ascii_name,
    l.country_code,
    l.admin1_code,
    l.admin2_code,
    l.feature_class,
    l.feature_code,
    l.population,
    l.timezone,
    GREATEST(
        similarity(l.name, search_text),
        similarity(COALESCE(l.ascii_name, ''), search_text)
    ) as similarity
FROM geonames.locations l
WHERE (
    l.name % search_text OR 
    COALESCE(l.ascii_name, '') % search_text
)
AND (country_code IS NULL OR l.country_code = country_code)
AND (feature_class IS NULL OR l.feature_class = feature_class)
AND (min_population IS NULL OR l.population >= min_population)
ORDER BY similarity DESC, l.population DESC NULLS LAST
LIMIT limit_count;
$$ LANGUAGE SQL STABLE; 