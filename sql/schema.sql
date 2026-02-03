-- Star Schema for NYC Traffic Safety Analysis

-- Dimension: Date/Time (Optimized for NYC timezone)
CREATE TABLE dim_datetime (
    datetime_id INT AUTO_INCREMENT PRIMARY KEY,
    datetime_utc DATETIME NOT NULL,          -- From weather API (UTC)
    datetime_nyc DATETIME NOT NULL,          -- Converted to NYC time
    date_nyc DATE NOT NULL,                  -- Date in NYC time
    hour_nyc INT NOT NULL,                   -- Hour (0-23) in NYC time
    day_of_week VARCHAR(10) NOT NULL,
    day_of_month INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_rush_hour BOOLEAN NOT NULL,          -- 7-10 AM, 4-7 PM
    is_night BOOLEAN NOT NULL,               -- 8 PM - 6 AM
    quarter INT NOT NULL,
    season VARCHAR(10) NOT NULL,             -- Winter, Spring, Summer, Fall
    UNIQUE KEY uk_datetime_nyc (datetime_nyc),
    INDEX idx_datetime_utc (datetime_utc),
    INDEX idx_date_nyc (date_nyc),
    INDEX idx_hour_nyc (hour_nyc),
    INDEX idx_weekend (is_weekend),
    INDEX idx_rush_hour (is_rush_hour),
    INDEX idx_season (season)
) ENGINE=InnoDB;

-- Dimension: Location (EXACT match for your collision data columns)
CREATE TABLE dim_location (
    location_id INT AUTO_INCREMENT PRIMARY KEY,
    borough VARCHAR(50),
    zip_code VARCHAR(10),
    latitude DECIMAL(12, 9),                 -- Match your precision: 40.576540
    longitude DECIMAL(12, 9),                -- Match your precision: -74.166435
    location_description TEXT,               -- Your "location" column
    on_street_name VARCHAR(255),
    off_street_name VARCHAR(255),
    cross_street_name VARCHAR(255),
    has_coordinates BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_lat_lon (latitude, longitude),  -- Prevent duplicate coordinates
    INDEX idx_borough (borough),
    INDEX idx_zip (zip_code),
    INDEX idx_coordinates (latitude, longitude),
    INDEX idx_street (on_street_name(50))
) ENGINE=InnoDB;

-- Dimension: Vehicle Types (EXACT match for your columns)
CREATE TABLE dim_vehicle_types (
    vehicle_type_id INT AUTO_INCREMENT PRIMARY KEY,
    vehicle_type_code VARCHAR(100) NOT NULL,  -- Your exact codes: 'Sedan', 'E-Bike'
    vehicle_category VARCHAR(50),             -- Grouped category
    is_motorized BOOLEAN DEFAULT TRUE,
    is_commercial BOOLEAN DEFAULT FALSE,
    description TEXT,
    UNIQUE KEY uk_vehicle_code (vehicle_type_code),
    INDEX idx_category (vehicle_category),
    INDEX idx_motorized (is_motorized)
) ENGINE=InnoDB;

-- Dimension: Contributing Factors (EXACT match for your columns)
CREATE TABLE dim_contributing_factors (
    factor_id INT AUTO_INCREMENT PRIMARY KEY,
    factor_code VARCHAR(255) NOT NULL,       -- Your exact codes: 'Unspecified', 'Driver Inattention/Distraction'
    factor_description TEXT,
    severity_level VARCHAR(20),              -- HIGH, MEDIUM, LOW
    is_preventable BOOLEAN DEFAULT TRUE,
    requires_action BOOLEAN DEFAULT FALSE,
    UNIQUE KEY uk_factor_code (factor_code),
    INDEX idx_severity (severity_level),
    INDEX idx_preventable (is_preventable)
) ENGINE=InnoDB;

-- Dimension: Weather Conditions
CREATE TABLE dim_weather_conditions (
    weather_id INT AUTO_INCREMENT PRIMARY KEY,
    condition_category VARCHAR(50) NOT NULL,
    severity_level VARCHAR(20) NOT NULL,
    temperature_range VARCHAR(50),
    visibility_range VARCHAR(50),
    precipitation_level VARCHAR(50),
    wind_level VARCHAR(50),
    description TEXT,
    safety_score INT DEFAULT 100,            -- 0-100, lower = more dangerous
    UNIQUE KEY uk_condition (condition_category, severity_level),
    INDEX idx_category (condition_category),
    INDEX idx_safety (safety_score)
) ENGINE=InnoDB;

-- Fact Table: Weather Measurements (EXACT match for your weather data)
CREATE TABLE fact_weather (
    weather_fact_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    datetime_id INT NOT NULL,
    location_id INT NOT NULL,                -- Links to borough
    weather_id INT,
    temperature_2m DECIMAL(6, 2),            -- Your data: 6.0699997 -> 6.07
    precipitation DECIMAL(6, 2),             -- mm
    visibility INT,                          -- meters (your data: 31000)
    rain DECIMAL(6, 2),                      -- mm
    showers DECIMAL(6, 2),                   -- mm
    snowfall DECIMAL(6, 2),                  -- cm
    wind_speed_10m DECIMAL(6, 2),           -- km/h (your data: 7.42159)
    is_adverse_weather BOOLEAN DEFAULT FALSE, -- Low vis, heavy precip, high wind
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (datetime_id) REFERENCES dim_datetime(datetime_id),
    FOREIGN KEY (location_id) REFERENCES dim_location(location_id),
    FOREIGN KEY (weather_id) REFERENCES dim_weather_conditions(weather_id),
    UNIQUE KEY uk_weather_record (datetime_id, location_id),
    INDEX idx_temperature (temperature_2m),
    INDEX idx_visibility (visibility),
    INDEX idx_adverse (is_adverse_weather),
    INDEX idx_datetime_location (datetime_id, location_id)
) ENGINE=InnoDB;

-- Fact Table: Collisions (EXACT match for your collision data)
CREATE TABLE fact_collisions (
    collision_fact_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    -- Your exact collision_id column
    collision_id VARCHAR(50) UNIQUE NOT NULL,  -- Your data: 4871595, 4871819
    
    -- Time reference (will be linked after transformation)
    datetime_id INT NOT NULL,
    
    -- Location reference
    location_id INT NOT NULL,
    
    -- Weather reference (will be linked during ETL)
    weather_id INT,
    
    -- Your EXACT injury/killed columns
    persons_injured INT DEFAULT 0,           -- number_of_persons_injured
    persons_killed INT DEFAULT 0,            -- number_of_persons_killed
    pedestrians_injured INT DEFAULT 0,       -- number_of_pedestrians_injured
    pedestrians_killed INT DEFAULT 0,        -- number_of_pedestrians_killed
    cyclists_injured INT DEFAULT 0,          -- number_of_cyclist_injured
    cyclists_killed INT DEFAULT 0,           -- number_of_cyclist_killed
    motorists_injured INT DEFAULT 0,         -- number_of_motorist_injured
    motorists_killed INT DEFAULT 0,          -- number_of_motorist_killed
    
    -- Computed fields
    total_involved INT DEFAULT 0,
    has_injuries BOOLEAN DEFAULT FALSE,
    has_fatalities BOOLEAN DEFAULT FALSE,
    severity_level VARCHAR(20) DEFAULT 'NONE', -- NONE, MINOR, MODERATE, SEVERE, FATAL
    
    -- Contributing factors (your exact 5 columns)
    contributing_factor_1_id INT,            -- contributing_factor_vehicle_1
    contributing_factor_2_id INT,            -- contributing_factor_vehicle_2
    contributing_factor_3_id INT,
    contributing_factor_4_id INT,
    contributing_factor_5_id INT,
    
    -- Vehicle types (your exact 5 columns)
    vehicle_type_1_id INT,                   -- vehicle_type_code1
    vehicle_type_2_id INT,                   -- vehicle_type_code2
    vehicle_type_3_id INT,                   -- vehicle_type_code_3
    vehicle_type_4_id INT,                   -- vehicle_type_code_4
    vehicle_type_5_id INT,                   -- vehicle_type_code_5
    
    -- Metadata
    number_of_vehicles INT DEFAULT 1,
    data_source VARCHAR(50) DEFAULT 'NYC_OPEN_DATA',
    raw_crash_date DATE,                     -- Original crash_date for debugging
    raw_crash_time TIME,                     -- Original crash_time for debugging
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    -- Foreign keys
    FOREIGN KEY (datetime_id) REFERENCES dim_datetime(datetime_id),
    FOREIGN KEY (location_id) REFERENCES dim_location(location_id),
    FOREIGN KEY (weather_id) REFERENCES dim_weather_conditions(weather_id),
    
    -- Vehicle type foreign keys
    FOREIGN KEY (vehicle_type_1_id) REFERENCES dim_vehicle_types(vehicle_type_id),
    FOREIGN KEY (vehicle_type_2_id) REFERENCES dim_vehicle_types(vehicle_type_id),
    FOREIGN KEY (vehicle_type_3_id) REFERENCES dim_vehicle_types(vehicle_type_id),
    FOREIGN KEY (vehicle_type_4_id) REFERENCES dim_vehicle_types(vehicle_type_id),
    FOREIGN KEY (vehicle_type_5_id) REFERENCES dim_vehicle_types(vehicle_type_id),
    
    -- Contributing factor foreign keys
    FOREIGN KEY (contributing_factor_1_id) REFERENCES dim_contributing_factors(factor_id),
    FOREIGN KEY (contributing_factor_2_id) REFERENCES dim_contributing_factors(factor_id),
    FOREIGN KEY (contributing_factor_3_id) REFERENCES dim_contributing_factors(factor_id),
    FOREIGN KEY (contributing_factor_4_id) REFERENCES dim_contributing_factors(factor_id),
    FOREIGN KEY (contributing_factor_5_id) REFERENCES dim_contributing_factors(factor_id),
    
    -- Indexes for performance
    INDEX idx_collision_id (collision_id),
    INDEX idx_datetime (datetime_id),
    INDEX idx_location (location_id),
    INDEX idx_severity (severity_level),
    INDEX idx_injuries (has_injuries),
    INDEX idx_fatalities (has_fatalities),
    INDEX idx_weather (weather_id),
    INDEX idx_created (created_at)
) ENGINE=InnoDB;

-- Aggregate Table: Hourly Statistics
CREATE TABLE agg_hourly_stats (
    stats_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    datetime_id INT NOT NULL,
    location_id INT NOT NULL,
    weather_id INT,
    
    -- Collision statistics
    total_collisions INT DEFAULT 0,
    injury_collisions INT DEFAULT 0,
    fatal_collisions INT DEFAULT 0,
    total_injuries INT DEFAULT 0,
    total_fatalities INT DEFAULT 0,
    
    -- Injury breakdown
    pedestrian_injuries INT DEFAULT 0,
    cyclist_injuries INT DEFAULT 0,
    motorist_injuries INT DEFAULT 0,
    
    -- Weather statistics
    avg_temperature DECIMAL(6, 2),
    min_temperature DECIMAL(6, 2),
    max_temperature DECIMAL(6, 2),
    avg_visibility INT,
    min_visibility INT,
    total_precipitation DECIMAL(6, 2),
    avg_wind_speed DECIMAL(6, 2),
    max_wind_speed DECIMAL(6, 2),
    
    -- Rates per hour
    collision_rate_per_hour DECIMAL(10, 4),
    injury_rate_per_collision DECIMAL(10, 4),
    fatality_rate_per_collision DECIMAL(10, 4),
    
    -- Flags
    has_adverse_weather BOOLEAN DEFAULT FALSE,
    is_high_risk_hour BOOLEAN DEFAULT FALSE,
    
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    FOREIGN KEY (datetime_id) REFERENCES dim_datetime(datetime_id),
    FOREIGN KEY (location_id) REFERENCES dim_location(location_id),
    FOREIGN KEY (weather_id) REFERENCES dim_weather_conditions(weather_id),
    
    UNIQUE KEY uk_datetime_location (datetime_id, location_id),
    
    INDEX idx_collision_count (total_collisions DESC),
    INDEX idx_injury_count (total_injuries DESC),
    INDEX idx_high_risk (is_high_risk_hour),
    INDEX idx_adverse_weather (has_adverse_weather),
    INDEX idx_datetime_location (datetime_id, location_id)
) ENGINE=InnoDB;

-- Logging Table
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
    INDEX idx_run (pipeline_run_id),
    INDEX idx_status (status),
    INDEX idx_started (started_at)
) ENGINE=InnoDB;