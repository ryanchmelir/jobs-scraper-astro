-- supabase/migrations/00000000000005_geonames.sql

CREATE SCHEMA IF NOT EXISTS geonames;


-- First ensure we have required extensions
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Base tables first, then dependent tables
CREATE TABLE IF NOT EXISTS geonames.feature_codes (
    class CHAR(1) NOT NULL,
    code VARCHAR(10) NOT NULL,
    name TEXT NOT NULL,
    description TEXT,
    PRIMARY KEY (class, code)
);

CREATE TABLE IF NOT EXISTS geonames.country_info (
    iso CHAR(2) PRIMARY KEY,
    iso3 CHAR(3),
    iso_numeric INTEGER,
    fips VARCHAR(3),
    country TEXT NOT NULL,
    capital TEXT,
    area_sq_km DECIMAL,
    population INTEGER,
    continent CHAR(2),
    tld VARCHAR(3),
    currency_code VARCHAR(3),
    currency_name TEXT,
    phone VARCHAR(20),
    postal_code_format TEXT,
    postal_code_regex TEXT,
    languages TEXT,
    geonameid INTEGER,
    neighbours TEXT,
    equivalent_fips_code TEXT
);

CREATE TABLE IF NOT EXISTS geonames.admin_codes (
    country_code CHAR(2) NOT NULL,
    admin1_code VARCHAR(20) NOT NULL,
    admin2_code VARCHAR(80),
    name TEXT NOT NULL,
    ascii_name TEXT NOT NULL,
    geonameid INTEGER,
    PRIMARY KEY (country_code, admin1_code, admin2_code)
);

-- Main locations table
CREATE TABLE IF NOT EXISTS geonames.locations (
    geonameid INTEGER PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    asciiname VARCHAR(200) NOT NULL,
    alternatenames TEXT,
    latitude DECIMAL(10,7) NOT NULL,
    longitude DECIMAL(10,7) NOT NULL,
    feature_class CHAR(1) NOT NULL,
    feature_code VARCHAR(10) NOT NULL,
    country_code CHAR(2) NOT NULL,
    cc2 VARCHAR(200),
    admin1_code VARCHAR(20),
    admin2_code VARCHAR(80),
    admin3_code VARCHAR(20),
    admin4_code VARCHAR(20),
    population BIGINT,
    elevation INTEGER,
    dem INTEGER,
    timezone VARCHAR(40),
    modification_date DATE,
    geom geometry(Point, 4326) GENERATED ALWAYS AS (ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)) STORED,
    FOREIGN KEY (feature_class, feature_code) REFERENCES geonames.feature_codes (class, code),
    FOREIGN KEY (country_code) REFERENCES geonames.country_info (iso)
);

CREATE TABLE IF NOT EXISTS geonames.alternate_names (
    alternatenameid INTEGER PRIMARY KEY,
    geonameid INTEGER NOT NULL REFERENCES geonames.locations(geonameid),
    isolanguage VARCHAR(7),
    alternate_name VARCHAR(500) NOT NULL,
    is_preferred_name BOOLEAN DEFAULT false,
    is_short_name BOOLEAN DEFAULT false,
    is_colloquial BOOLEAN DEFAULT false,
    is_historic BOOLEAN DEFAULT false,
    from_date DATE,
    to_date DATE
);

CREATE TABLE IF NOT EXISTS geonames.hierarchies (
    parent_id INTEGER NOT NULL REFERENCES geonames.locations(geonameid),
    child_id INTEGER NOT NULL REFERENCES geonames.locations(geonameid),
    type VARCHAR(50) NOT NULL,
    PRIMARY KEY (parent_id, child_id, type)
);

CREATE TABLE IF NOT EXISTS geonames.boundaries (
    geonameid INTEGER PRIMARY KEY REFERENCES geonames.locations(geonameid),
    geom geometry(MultiPolygon, 4326)
);

CREATE TABLE IF NOT EXISTS geonames.timezones (
    timezone_id VARCHAR(40) PRIMARY KEY,
    country_code CHAR(2) REFERENCES geonames.country_info(iso),
    gmt_offset DECIMAL(3,1),
    dst_offset DECIMAL(3,1),
    raw_offset DECIMAL(3,1)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS locations_name_trgm_idx ON geonames.locations USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS locations_asciiname_trgm_idx ON geonames.locations USING gin (asciiname gin_trgm_ops);
CREATE INDEX IF NOT EXISTS locations_geom_idx ON geonames.locations USING gist (geom);
CREATE INDEX IF NOT EXISTS locations_country_admin1_idx ON geonames.locations(country_code, admin1_code);
CREATE INDEX IF NOT EXISTS locations_feature_idx ON geonames.locations(feature_class, feature_code);

CREATE INDEX IF NOT EXISTS alternate_names_name_trgm_idx ON geonames.alternate_names USING gin (alternate_name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS alternate_names_geonameid_idx ON geonames.alternate_names(geonameid);
CREATE INDEX IF NOT EXISTS alternate_names_language_idx ON geonames.alternate_names(isolanguage);

CREATE INDEX IF NOT EXISTS hierarchies_parent_idx ON geonames.hierarchies(parent_id);
CREATE INDEX IF NOT EXISTS hierarchies_child_idx ON geonames.hierarchies(child_id);
CREATE INDEX IF NOT EXISTS hierarchies_type_idx ON geonames.hierarchies(type);

CREATE INDEX IF NOT EXISTS boundaries_geom_idx ON geonames.boundaries USING gist (geom);

-- Helper functions for location queries
CREATE OR REPLACE FUNCTION geonames.find_location(
    search_text TEXT,
    in_country_code CHAR(2) DEFAULT NULL,
    in_feature_class CHAR(1) DEFAULT NULL
) RETURNS TABLE (
    geonameid INTEGER,
    name TEXT,
    country_code CHAR(2),
    admin1_code VARCHAR(20),
    feature_class CHAR(1),
    feature_code VARCHAR(10),
    population BIGINT,
    similarity FLOAT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        l.geonameid,
        l.name,
        l.country_code,
        l.admin1_code,
        l.feature_class,
        l.feature_code,
        l.population,
        similarity(l.name, search_text) as similarity
    FROM geonames.locations l
    WHERE (in_country_code IS NULL OR l.country_code = in_country_code)
    AND (in_feature_class IS NULL OR l.feature_class = in_feature_class)
    AND (
        l.name % search_text
        OR l.asciiname % search_text
        OR EXISTS (
            SELECT 1 FROM geonames.alternate_names an
            WHERE an.geonameid = l.geonameid
            AND an.alternate_name % search_text
        )
    )
    ORDER BY 
        similarity DESC,
        population DESC NULLS LAST
    LIMIT 10;
END;
$$ LANGUAGE plpgsql;

-- RLS Policies
ALTER TABLE geonames.locations DISABLE ROW LEVEL SECURITY;
ALTER TABLE geonames.alternate_names DISABLE ROW LEVEL SECURITY;
ALTER TABLE geonames.hierarchies DISABLE ROW LEVEL SECURITY;
ALTER TABLE geonames.boundaries DISABLE ROW LEVEL SECURITY;