USE nyc_traffic_safety;

-- ============================================================
-- DROP TABLES (facts first, then dims)
-- ============================================================

DROP TABLE IF EXISTS fact_collisions;
DROP TABLE IF EXISTS fact_weather;
DROP TABLE IF EXISTS dim_contributing_factors;
DROP TABLE IF EXISTS dim_vehicle_types;
DROP TABLE IF EXISTS dim_weather_conditions;
DROP TABLE IF EXISTS dim_location;
DROP TABLE IF EXISTS dim_datetime;
DROP TABLE IF EXISTS pipeline_logs;
DROP TABLE IF EXISTS agg_hourly_stats;


-- ============================================================
-- DIMENSION: DateTime
-- ============================================================

CREATE TABLE dim_datetime (

    datetime_id INT AUTO_INCREMENT PRIMARY KEY,

    datetime_utc DATETIME NOT NULL,
    datetime_nyc DATETIME NOT NULL,

    date_nyc DATE NOT NULL,
    hour_nyc TINYINT NOT NULL,

    day_of_week VARCHAR(10) NOT NULL,
    day_of_month TINYINT NOT NULL,
    month TINYINT NOT NULL,
    year SMALLINT NOT NULL,
    quarter TINYINT NOT NULL,
    season VARCHAR(10) NOT NULL,

    is_weekend BOOLEAN NOT NULL,
    is_rush_hour BOOLEAN NOT NULL,
    is_night BOOLEAN NOT NULL,

    UNIQUE KEY uk_datetime_nyc (datetime_nyc),

    INDEX idx_date_nyc (date_nyc),
    INDEX idx_date_hour (date_nyc, hour_nyc)

) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ============================================================
-- DIMENSION: Location
-- ============================================================

CREATE TABLE dim_location (

    location_id INT AUTO_INCREMENT PRIMARY KEY,

    borough VARCHAR(50) NOT NULL,

    UNIQUE KEY uk_borough (borough),
    INDEX idx_borough (borough)

) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ============================================================
-- FACT: Weather (weather category stored directly)
-- ============================================================

CREATE TABLE fact_weather (

    weather_fact_id BIGINT AUTO_INCREMENT PRIMARY KEY,

    datetime_id INT NOT NULL,
    location_id INT NOT NULL,

    temperature_2m DECIMAL(6,2),
    precipitation DECIMAL(6,2),
    visibility INT,
    rain DECIMAL(6,2),
    showers DECIMAL(6,2),
    snowfall DECIMAL(6,2),
    wind_speed_10m DECIMAL(6,2),

    weather_category VARCHAR(50),
    weather_severity VARCHAR(20),

    is_adverse_weather BOOLEAN DEFAULT FALSE,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE KEY uk_weather_record (datetime_id, location_id),

    CONSTRAINT fk_weather_datetime
        FOREIGN KEY (datetime_id)
        REFERENCES dim_datetime(datetime_id),

    CONSTRAINT fk_weather_location
        FOREIGN KEY (location_id)
        REFERENCES dim_location(location_id)

) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ============================================================
-- FACT: Collisions (vehicle + contributing factors stored directly)
-- ============================================================

CREATE TABLE fact_collisions (

    collision_fact_id BIGINT AUTO_INCREMENT PRIMARY KEY,

    collision_id VARCHAR(50) NOT NULL,

    datetime_id INT NOT NULL,
    location_id INT NOT NULL,

    persons_injured INT DEFAULT 0,
    persons_killed INT DEFAULT 0,

    pedestrians_injured INT DEFAULT 0,
    pedestrians_killed INT DEFAULT 0,

    cyclists_injured INT DEFAULT 0,
    cyclists_killed INT DEFAULT 0,

    motorists_injured INT DEFAULT 0,
    motorists_killed INT DEFAULT 0,

    total_involved INT DEFAULT 0,

    has_injuries BOOLEAN,
    has_fatalities BOOLEAN,

    severity_level VARCHAR(20),

    contributing_factor_vehicle_1 VARCHAR(255),
    contributing_factor_vehicle_2 VARCHAR(255),
    contributing_factor_vehicle_3 VARCHAR(255),
    contributing_factor_vehicle_4 VARCHAR(255),
    contributing_factor_vehicle_5 VARCHAR(255),

    vehicle_type_code1 VARCHAR(100),
    vehicle_type_code2 VARCHAR(100),
    vehicle_type_code_3 VARCHAR(100),
    vehicle_type_code_4 VARCHAR(100),
    vehicle_type_code_5 VARCHAR(100),

    number_of_vehicles INT,

    raw_crash_date DATE,
    raw_crash_time TIME,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE KEY uk_collision_id (collision_id),

    CONSTRAINT fk_collision_datetime
        FOREIGN KEY (datetime_id)
        REFERENCES dim_datetime(datetime_id),

    CONSTRAINT fk_collision_location
        FOREIGN KEY (location_id)
        REFERENCES dim_location(location_id)

) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

