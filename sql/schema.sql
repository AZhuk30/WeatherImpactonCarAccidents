-- Star Schema for NYC Traffic Safety Analysis
/*
NYC Traffic Safety Star Schema (Optimized)

NOTES (based on core SQL fundamentals / tuning mindset):
- Keep indexes that support: PRIMARY KEYS, UNIQUE KEYS, FOREIGN KEYS, and common query paths.
- Avoid “indexing everything” (especially low-cardinality BOOLEAN columns), because each extra index adds write overhead
  during ETL loads (INSERT/UPDATE/DELETE) and can slow your pipeline.
- Prefer composite indexes that match how you filter/join (ex: datetime_id + location_id) instead of many single-column indexes.
- Remove redundant indexes: a UNIQUE constraint already creates an index; don’t add another index on the same columns.

This script:
1) Drops tables in dependency order
2) Recreates tables with a lean, practical index strategy
3) Adds “notes” in comments for why each design choice exists

*/
-- ------------------------------------------------------------
-- DROP TABLES (facts/aggregates first, then dimensions)
-- ------------------------------------------------------------
DROP TABLE IF EXISTS pipeline_logs;
DROP TABLE IF EXISTS agg_hourly_stats;
DROP TABLE IF EXISTS fact_collisions;
DROP TABLE IF EXISTS fact_weather;

DROP TABLE IF EXISTS dim_weather_conditions;
DROP TABLE IF EXISTS dim_contributing_factors;
DROP TABLE IF EXISTS dim_vehicle_types;
DROP TABLE IF EXISTS dim_location;
DROP TABLE IF EXISTS dim_datetime;

-- ------------------------------------------------------------
-- DIMENSION: Date/Time (NYC timezone derived fields)
-- ------------------------------------------------------------
CREATE TABLE dim_datetime (
    datetime_id INT AUTO_INCREMENT PRIMARY KEY,

    -- Store both UTC (source) and NYC (business time)
    datetime_utc DATETIME NOT NULL,
    datetime_nyc DATETIME NOT NULL,

    -- Common analytics filters
    date_nyc DATE NOT NULL,
    hour_nyc TINYINT NOT NULL,   -- 0-23 fits in TINYINT

    -- Descriptive attributes
    day_of_week VARCHAR(10) NOT NULL,
    day_of_month TINYINT NOT NULL,
    month TINYINT NOT NULL,
    year SMALLINT NOT NULL,
    quarter TINYINT NOT NULL,
    season VARCHAR(10) NOT NULL,

    -- Flags (LOW cardinality: do NOT index by default)
    is_weekend BOOLEAN NOT NULL,
    is_rush_hour BOOLEAN NOT NULL,
    is_night BOOLEAN NOT NULL,

    -- Unique business key: one row per NYC timestamp
    UNIQUE KEY uk_datetime_nyc (datetime_nyc),

    -- Indexes: keep what you actually filter/join on
    INDEX idx_datetime_utc (datetime_utc),
    INDEX idx_date_nyc (date_nyc),

    -- Composite index for typical “date range + hour of day” analytics
    INDEX idx_date_hour (date_nyc, hour_nyc)

    -- NOTE: intentionally removed single-column idx_hour_nyc and boolean indexes.
) ENGINE=InnoDB;

-- ------------------------------------------------------------
-- DIMENSION: Location
-- ------------------------------------------------------------
CREATE TABLE dim_location (
    location_id INT AUTO_INCREMENT PRIMARY KEY,

    borough VARCHAR(50),
    zip_code VARCHAR(10),

    -- Match precision from source
    latitude DECIMAL(12, 9),
    longitude DECIMAL(12, 9),

    location_description TEXT,
    on_street_name VARCHAR(255),
    off_street_name VARCHAR(255),
    cross_street_name VARCHAR(255),

    has_coordinates BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    /*
      NOTE:
      - DO NOT enforce UNIQUE(latitude, longitude) for collision data.
      - Many records can share rounded/approx coords or be missing.
      - Keep a normal index for geo lookups instead.
    */
    INDEX idx_lat_lon (latitude, longitude),
    INDEX idx_borough (borough),
    INDEX idx_zip (zip_code),

    -- Prefix index to support street searches without huge index size
    INDEX idx_on_street_prefix (on_street_name(50))
) ENGINE=InnoDB;

-- ------------------------------------------------------------
-- DIMENSION: Vehicle Types
-- ------------------------------------------------------------
CREATE TABLE dim_vehicle_types (
    vehicle_type_id INT AUTO_INCREMENT PRIMARY KEY,
    vehicle_type_code VARCHAR(100) NOT NULL,  -- exact codes
    vehicle_category VARCHAR(50),
    is_motorized BOOLEAN DEFAULT TRUE,
    is_commercial BOOLEAN DEFAULT FALSE,
    description TEXT,

    -- Business key
    UNIQUE KEY uk_vehicle_code (vehicle_type_code),

    -- Keep category if used in dashboards; drop boolean indexes unless proven
    INDEX idx_vehicle_category (vehicle_category)
) ENGINE=InnoDB;

-- ------------------------------------------------------------
-- DIMENSION: Contributing Factors
-- ------------------------------------------------------------
CREATE TABLE dim_contributing_factors (
    factor_id INT AUTO_INCREMENT PRIMARY KEY,
    factor_code VARCHAR(255) NOT NULL,
    factor_description TEXT,
    severity_level VARCHAR(20),
    is_preventable BOOLEAN DEFAULT TRUE,
    requires_action BOOLEAN DEFAULT FALSE,

    UNIQUE KEY uk_factor_code (factor_code),

    -- Severity can be useful for filtering/grouping; booleans not indexed by default
    INDEX idx_factor_severity (severity_level)
) ENGINE=InnoDB;

-- ------------------------------------------------------------
-- DIMENSION: Weather Conditions
-- ------------------------------------------------------------
CREATE TABLE dim_weather_conditions (
    weather_id INT AUTO_INCREMENT PRIMARY KEY,
    condition_category VARCHAR(50) NOT NULL,
    severity_level VARCHAR(20) NOT NULL,
    temperature_range VARCHAR(50),
    visibility_range VARCHAR(50),
    precipitation_level VARCHAR(50),
    wind_level VARCHAR(50),
    description TEXT,
    safety_score INT DEFAULT 100,

    -- Business key: category + severity
    UNIQUE KEY uk_condition (condition_category, severity_level),

    -- Helpful filters
    INDEX idx_weather_category (condition_category),
    INDEX idx_safety_score (safety_score)
) ENGINE=InnoDB;

-- ------------------------------------------------------------
-- FACT: Weather Measurements
-- ------------------------------------------------------------
CREATE TABLE fact_weather (
    weather_fact_id BIGINT AUTO_INCREMENT PRIMARY KEY,

    datetime_id INT NOT NULL,
    location_id INT NOT NULL,
    weather_id INT,

    temperature_2m DECIMAL(6, 2),
    precipitation DECIMAL(6, 2),
    visibility INT,
    rain DECIMAL(6, 2),
    showers DECIMAL(6, 2),
    snowfall DECIMAL(6, 2),
    wind_speed_10m DECIMAL(6, 2),

    is_adverse_weather BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Enforce one weather record per hour+borough
    UNIQUE KEY uk_weather_record (datetime_id, location_id),

    -- Foreign keys (InnoDB requires parent key indexed; the child side is supported by uk_weather_record)
    CONSTRAINT fk_fw_datetime FOREIGN KEY (datetime_id) REFERENCES dim_datetime(datetime_id),
    CONSTRAINT fk_fw_location FOREIGN KEY (location_id) REFERENCES dim_location(location_id),
    CONSTRAINT fk_fw_weather  FOREIGN KEY (weather_id)   REFERENCES dim_weather_conditions(weather_id)

    /*
      NOTES:
      - Removed redundant INDEX(datetime_id, location_id) because UNIQUE already creates the index.
      - Removed measure indexes (temperature_2m/visibility) to speed ETL writes.
        Add them back later ONLY if EXPLAIN shows they’re needed for frequent filters.
      - If you often query adverse weather by time range, consider a composite:
        (is_adverse_weather, datetime_id) rather than indexing boolean alone.
    */
) ENGINE=InnoDB;

-- ------------------------------------------------------------
-- FACT: Collisions
-- ------------------------------------------------------------
CREATE TABLE fact_collisions (
    collision_fact_id BIGINT AUTO_INCREMENT PRIMARY KEY,

    -- Natural unique key from NYC Open Data
    collision_id VARCHAR(50) NOT NULL,
    UNIQUE KEY uk_collision_id (collision_id),

    datetime_id INT NOT NULL,
    location_id INT NOT NULL,
    weather_id INT,

    persons_injured INT DEFAULT 0,
    persons_killed INT DEFAULT 0,
    pedestrians_injured INT DEFAULT 0,
    pedestrians_killed INT DEFAULT 0,
    cyclists_injured INT DEFAULT 0,
    cyclists_killed INT DEFAULT 0,
    motorists_injured INT DEFAULT 0,
    motorists_killed INT DEFAULT 0,

    total_involved INT DEFAULT 0,
    has_injuries BOOLEAN DEFAULT FALSE,
    has_fatalities BOOLEAN DEFAULT FALSE,
    severity_level VARCHAR(20) DEFAULT 'NONE',

    contributing_factor_1_id INT,
    contributing_factor_2_id INT,
    contributing_factor_3_id INT,
    contributing_factor_4_id INT,
    contributing_factor_5_id INT,

    vehicle_type_1_id INT,
    vehicle_type_2_id INT,
    vehicle_type_3_id INT,
    vehicle_type_4_id INT,
    vehicle_type_5_id INT,

    number_of_vehicles INT DEFAULT 1,
    data_source VARCHAR(50) DEFAULT 'NYC_OPEN_DATA',
    raw_crash_date DATE,
    raw_crash_time TIME,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    -- Foreign keys
    CONSTRAINT fk_fc_datetime FOREIGN KEY (datetime_id) REFERENCES dim_datetime(datetime_id),
    CONSTRAINT fk_fc_location FOREIGN KEY (location_id) REFERENCES dim_location(location_id),
    CONSTRAINT fk_fc_weather  FOREIGN KEY (weather_id)   REFERENCES dim_weather_conditions(weather_id),

    CONSTRAINT fk_fc_v1 FOREIGN KEY (vehicle_type_1_id) REFERENCES dim_vehicle_types(vehicle_type_id),
    CONSTRAINT fk_fc_v2 FOREIGN KEY (vehicle_type_2_id) REFERENCES dim_vehicle_types(vehicle_type_id),
    CONSTRAINT fk_fc_v3 FOREIGN KEY (vehicle_type_3_id) REFERENCES dim_vehicle_types(vehicle_type_id),
    CONSTRAINT fk_fc_v4 FOREIGN KEY (vehicle_type_4_id) REFERENCES dim_vehicle_types(vehicle_type_id),
    CONSTRAINT fk_fc_v5 FOREIGN KEY (vehicle_type_5_id) REFERENCES dim_vehicle_types(vehicle_type_id),

    CONSTRAINT fk_fc_f1 FOREIGN KEY (contributing_factor_1_id) REFERENCES dim_contributing_factors(factor_id),
    CONSTRAINT fk_fc_f2 FOREIGN KEY (contributing_factor_2_id) REFERENCES dim_contributing_factors(factor_id),
    CONSTRAINT fk_fc_f3 FOREIGN KEY (contributing_factor_3_id) REFERENCES dim_contributing_factors(factor_id),
    CONSTRAINT fk_fc_f4 FOREIGN KEY (contributing_factor_4_id) REFERENCES dim_contributing_factors(factor_id),
    CONSTRAINT fk_fc_f5 FOREIGN KEY (contributing_factor_5_id) REFERENCES dim_contributing_factors(factor_id),

    /*
      INDEX STRATEGY (lean + practical):
      - Keep composite indexes matching your star-schema join/filter patterns.
      - Avoid indexing low-cardinality booleans alone (has_injuries/has_fatalities).
      - You can add specialty composites later if EXPLAIN shows a need.
    */

    -- Core join path: collisions often joined/grouped by time and borough
    INDEX idx_fc_dt_loc (datetime_id, location_id),
    INDEX idx_fc_loc_dt (location_id, datetime_id),

    -- Optional: if you filter by severity frequently
    INDEX idx_fc_severity (severity_level),

    -- If you filter by weather category via weather_id a lot
    INDEX idx_fc_weather (weather_id),

    -- Operational filtering
    INDEX idx_fc_created (created_at)

    /*
      NOTES:
      - Removed redundant INDEX(collision_id) because UNIQUE already creates it.
      - Removed single-column idx_datetime and idx_location because composites usually cover them.
        Keep singles only if EXPLAIN shows they’re used for a specific query.
    */
) ENGINE=InnoDB;

-- ------------------------------------------------------------
-- AGGREGATE: Hourly Statistics
-- ------------------------------------------------------------
CREATE TABLE agg_hourly_stats (
    stats_id BIGINT AUTO_INCREMENT PRIMARY KEY,

    datetime_id INT NOT NULL,
    location_id INT NOT NULL,
    weather_id INT,

    total_collisions INT DEFAULT 0,
    injury_collisions INT DEFAULT 0,
    fatal_collisions INT DEFAULT 0,
    total_injuries INT DEFAULT 0,
    total_fatalities INT DEFAULT 0,

    pedestrian_injuries INT DEFAULT 0,
    cyclist_injuries INT DEFAULT 0,
    motorist_injuries INT DEFAULT 0,

    avg_temperature DECIMAL(6, 2),
    min_temperature DECIMAL(6, 2),
    max_temperature DECIMAL(6, 2),
    avg_visibility INT,
    min_visibility INT,
    total_precipitation DECIMAL(6, 2),
    avg_wind_speed DECIMAL(6, 2),
    max_wind_speed DECIMAL(6, 2),

    collision_rate_per_hour DECIMAL(10, 4),
    injury_rate_per_collision DECIMAL(10, 4),
    fatality_rate_per_collision DECIMAL(10, 4),

    has_adverse_weather BOOLEAN DEFAULT FALSE,
    is_high_risk_hour BOOLEAN DEFAULT FALSE,

    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    CONSTRAINT fk_ahs_datetime FOREIGN KEY (datetime_id) REFERENCES dim_datetime(datetime_id),
    CONSTRAINT fk_ahs_location FOREIGN KEY (location_id) REFERENCES dim_location(location_id),
    CONSTRAINT fk_ahs_weather  FOREIGN KEY (weather_id)   REFERENCES dim_weather_conditions(weather_id),

    -- One row per hour + borough
    UNIQUE KEY uk_ahs_dt_loc (datetime_id, location_id),

    /*
      NOTES:
      - Removed redundant INDEX(datetime_id, location_id) because UNIQUE already creates it.
      - Avoid indexing “DESC” counts. Better to filter by date/location first, then ORDER BY in result.
      - Keep targeted flags only if used frequently with selective filters (otherwise they’re low-cardinality).
    */
    INDEX idx_ahs_high_risk (is_high_risk_hour),
    INDEX idx_ahs_adverse  (has_adverse_weather)
) ENGINE=InnoDB;

-- ------------------------------------------------------------
-- LOGGING: Pipeline Logs
-- ------------------------------------------------------------
CREATE TABLE pipeline_logs (
    log_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    pipeline_run_id VARCHAR(50) NOT NULL,
    stage VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL,
    message TEXT,
    records_processed INT,
    error_details TEXT,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,

    -- Useful operational filters
    INDEX idx_pl_run (pipeline_run_id),
    INDEX idx_pl_status (status),
    INDEX idx_pl_started (started_at)
) ENGINE=InnoDB;

-- End of NYC Traffic Safety Star Schema