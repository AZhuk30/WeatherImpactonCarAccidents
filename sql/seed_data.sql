-- Seed Location Dimension with NYC boroughs
/*
SEED DATA 

Design notes:
- Idempotent: safe to run multiple times.
- Uses WHERE NOT EXISTS when no UNIQUE constraint exists.
- Uses ON DUPLICATE KEY UPDATE where UNIQUE constraints already exist.
- No blank codes; uses explicit 'UNKNOWN' instead.
*/

-- ------------------------------------------------------------
-- Seed Location Dimension with NYC boroughs
-- (dim_location does NOT have UNIQUE(borough))
-- ------------------------------------------------------------
INSERT INTO dim_location (borough)
SELECT v.borough
FROM (
    SELECT 'MANHATTAN'     AS borough UNION ALL
    SELECT 'BROOKLYN'                UNION ALL
    SELECT 'QUEENS'                  UNION ALL
    SELECT 'BRONX'                   UNION ALL
    SELECT 'STATEN ISLAND'
) v
WHERE NOT EXISTS (
    SELECT 1
    FROM dim_location dl
    WHERE dl.borough = v.borough
);

-- ------------------------------------------------------------
-- Seed Vehicle Types
-- (Uses UNIQUE(vehicle_type_code))
-- ------------------------------------------------------------
INSERT INTO dim_vehicle_types (vehicle_type_code, vehicle_category, is_motorized)
VALUES
    ('Sedan', 'PASSENGER VEHICLE', TRUE),
    ('E-Bike', 'BICYCLE', FALSE),
    ('Station Wagon/Sport Utility Vehicle', 'PASSENGER VEHICLE', TRUE),
    ('Bicycle', 'BICYCLE', FALSE),
    ('Motorcycle', 'MOTORCYCLE', TRUE),
    ('Bus', 'COMMERCIAL', TRUE),
    ('Taxi', 'COMMERCIAL', TRUE),
    ('Box Truck', 'COMMERCIAL', TRUE),
    ('Ambulance', 'EMERGENCY', TRUE),
    ('Fire Truck', 'EMERGENCY', TRUE),
    ('Pick-up Truck', 'COMMERCIAL', TRUE),
    ('Van', 'COMMERCIAL', TRUE),
    ('Scooter', 'MOTORCYCLE', TRUE),
    ('UNKNOWN', 'UNKNOWN', FALSE)
ON DUPLICATE KEY UPDATE
    vehicle_category = VALUES(vehicle_category),
    is_motorized     = VALUES(is_motorized);

-- ------------------------------------------------------------
-- Seed Contributing Factors
-- (Uses UNIQUE(factor_code))
-- ------------------------------------------------------------
INSERT INTO dim_contributing_factors (factor_code, severity_level, is_preventable)
VALUES
    ('Unspecified', 'LOW', FALSE),
    ('Driver Inattention/Distraction', 'HIGH', TRUE),
    ('Failure to Yield Right-of-Way', 'HIGH', TRUE),
    ('Following Too Closely', 'MEDIUM', TRUE),
    ('Unsafe Speed', 'HIGH', TRUE),
    ('Backing Unsafely', 'LOW', TRUE),
    ('Passing or Lane Usage Improper', 'MEDIUM', TRUE),
    ('Turning Improperly', 'MEDIUM', TRUE),
    ('Traffic Control Disregarded', 'HIGH', TRUE),
    ('Alcohol Involvement', 'CRITICAL', TRUE),
    ('Drugs (Illegal)', 'CRITICAL', TRUE),
    ('Fatigued/Drowsy', 'MEDIUM', TRUE),
    ('View Obstructed/Limited', 'MEDIUM', FALSE),
    ('Pedestrian Error/Confusion', 'MEDIUM', TRUE),
    ('Pavement Slippery', 'MEDIUM', FALSE),
    ('Other Electronic Device', 'HIGH', TRUE),
    ('Aggressive Driving/Road Rage', 'HIGH', TRUE),
    ('Outside Car Distraction', 'MEDIUM', TRUE),
    ('Passenger Distraction', 'MEDIUM', TRUE),
    ('Glare', 'LOW', FALSE)
ON DUPLICATE KEY UPDATE
    severity_level = VALUES(severity_level),
    is_preventable = VALUES(is_preventable);

-- ------------------------------------------------------------
-- Seed Weather Conditions
-- (Uses UNIQUE(condition_category, severity_level))
-- ------------------------------------------------------------
INSERT INTO dim_weather_conditions (condition_category, severity_level, safety_score, description)
VALUES
    ('CLEAR', 'NORMAL',   95, 'Clear weather, good visibility'),
    ('CLEAR', 'COLD',     85, 'Clear but cold, possible ice'),
    ('CLEAR', 'HOT',      90, 'Clear and hot'),
    ('RAIN',  'LIGHT',    80, 'Light rain, slightly reduced visibility'),
    ('RAIN',  'MODERATE', 70, 'Moderate rain, reduced visibility and traction'),
    ('RAIN',  'HEAVY',    50, 'Heavy rain, poor visibility and traction'),
    ('SNOW',  'LIGHT',    65, 'Light snow, reduced traction'),
    ('SNOW',  'MODERATE', 40, 'Moderate snow, poor traction and visibility'),
    ('SNOW',  'HEAVY',    20, 'Heavy snow, very dangerous conditions'),
    ('FOG',   'LIGHT',    75, 'Light fog, reduced visibility'),
    ('FOG',   'MODERATE', 60, 'Moderate fog, poor visibility'),
    ('FOG',   'HEAVY',    30, 'Heavy fog, very poor visibility'),
    ('WIND',  'MODERATE', 85, 'Moderate wind, minimal impact'),
    ('WIND',  'STRONG',   60, 'Strong wind, affects vehicle control'),
    ('MIXED', 'SEVERE',   25, 'Multiple adverse conditions')
ON DUPLICATE KEY UPDATE
    safety_score = VALUES(safety_score),
    description  = VALUES(description);

-- ------------------------------------------------------------