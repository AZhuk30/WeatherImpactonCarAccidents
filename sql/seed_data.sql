-- Seed Location Dimension with NYC boroughs
INSERT IGNORE INTO dim_location (borough) VALUES
    ('MANHATTAN'),
    ('BROOKLYN'),
    ('QUEENS'),
    ('BRONX'),
    ('STATEN ISLAND');

-- Seed Vehicle Types with YOUR EXACT data from sample
INSERT IGNORE INTO dim_vehicle_types (vehicle_type_code, vehicle_category, is_motorized) VALUES
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
    ('', 'UNKNOWN', FALSE);

-- Seed Contributing Factors with YOUR EXACT data from sample
INSERT IGNORE INTO dim_contributing_factors (factor_code, severity_level, is_preventable) VALUES
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
    ('Glare', 'LOW', FALSE);

-- Seed Weather Conditions
INSERT IGNORE INTO dim_weather_conditions (condition_category, severity_level, safety_score, description) VALUES
    ('CLEAR', 'NORMAL', 95, 'Clear weather, good visibility'),
    ('CLEAR', 'COLD', 85, 'Clear but cold, possible ice'),
    ('CLEAR', 'HOT', 90, 'Clear and hot'),
    ('RAIN', 'LIGHT', 80, 'Light rain, slightly reduced visibility'),
    ('RAIN', 'MODERATE', 70, 'Moderate rain, reduced visibility and traction'),
    ('RAIN', 'HEAVY', 50, 'Heavy rain, poor visibility and traction'),
    ('SNOW', 'LIGHT', 65, 'Light snow, reduced traction'),
    ('SNOW', 'MODERATE', 40, 'Moderate snow, poor traction and visibility'),
    ('SNOW', 'HEAVY', 20, 'Heavy snow, very dangerous conditions'),
    ('FOG', 'LIGHT', 75, 'Light fog, reduced visibility'),
    ('FOG', 'MODERATE', 60, 'Moderate fog, poor visibility'),
    ('FOG', 'HEAVY', 30, 'Heavy fog, very poor visibility'),
    ('WIND', 'MODERATE', 85, 'Moderate wind, minimal impact'),
    ('WIND', 'STRONG', 60, 'Strong wind, affects vehicle control'),
    ('MIXED', 'SEVERE', 25, 'Multiple adverse conditions');