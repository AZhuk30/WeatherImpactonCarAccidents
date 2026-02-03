import sys
import pandas as pd
sys.path.append('.')

from src.transform import WeatherTransformer, CollisionsTransformer

def test_weather_transform():
    """Test weather transformation with your data format"""
    print("Testing Weather Transformation...")
    
    # Create sample data matching your format
    sample_weather = pd.DataFrame({
        'borough': ['MANHATTAN', 'MANHATTAN', 'BROOKLYN'],
        'datetime': ['2026-01-14 05:00:00+00:00', '2026-01-14 06:00:00+00:00', '2026-01-14 05:00:00+00:00'],
        'temperature_2m': [6.06999997, 6.06999997, 5.5],
        'precipitation': [0, 0, 0],
        'visibility': [31000, 28900, 30000],
        'rain': [0, 0, 0],
        'showers': [0, 0, 0],
        'snowfall': [0, 0, 0],
        'wind_speed_10m': [7.42159, 5.1544156, 6.0],
        'date': ['1/14/2026', '1/14/2026', '1/14/2026']
    })
    
    transformer = WeatherTransformer()
    result = transformer.transform(sample_weather)
    
    print(f"✅ Weather transformation successful")
    print(f"Input shape: {sample_weather.shape}")
    print(f"Output shape: {result.shape}")
    print(f"\nResult columns: {list(result.columns)}")
    print(f"\nSample row:\n{result.iloc[0]}")
    
    return result

def test_collisions_transform():
    """Test collisions transformation with your data format"""
    print("\n" + "="*50)
    print("Testing Collisions Transformation...")
    
    # Create sample data matching YOUR EXACT format
    sample_collisions = pd.DataFrame({
        'crash_date': ['2026-01-14T00:00:00.000', '2026-01-14T00:00:00.000'],
        'crash_time': ['11:00', '7:00'],
        'borough': ['', 'MANHATTAN'],  # Note: First has empty borough
        'zip_code': ['', '10011'],
        'latitude': [40.57654, 40.742283],
        'longitude': [-74.166435, -74.00442],
        'location': ['",\n(40.57654, -74.166435)"', '",\n(40.742283, -74.00442)"'],
        'on_street_name': ['', 'W 16 ST'],
        'off_street_name': ['2795      RICHMOND AVE', '9 AVE'],
        'cross_street_name': ['', ''],
        'number_of_persons_injured': [0, 1],
        'number_of_persons_killed': [0, 0],
        'number_of_pedestrians_injured': [0, 1],
        'number_of_pedestrians_killed': [0, 0],
        'number_of_cyclist_injured': [0, 0],
        'number_of_cyclist_killed': [0, 0],
        'number_of_motorist_injured': [0, 0],
        'number_of_motorist_killed': [0, 0],
        'contributing_factor_vehicle_1': ['Unspecified', 'Driver Inattention/Distraction'],
        'contributing_factor_vehicle_2': ['', ''],
        'contributing_factor_vehicle_3': ['', ''],
        'contributing_factor_vehicle_4': ['', ''],
        'contributing_factor_vehicle_5': ['', ''],
        'collision_id': ['4871595', '4871819'],
        'vehicle_type_code1': ['Sedan', 'E-Bike'],
        'vehicle_type_code2': ['', ''],
        'vehicle_type_code_3': ['', ''],
        'vehicle_type_code_4': ['', ''],
        'vehicle_type_code_5': ['', '']
    })
    
    transformer = CollisionsTransformer()
    result = transformer.transform(sample_collisions)
    
    print(f"✅ Collisions transformation successful")
    print(f"Input shape: {sample_collisions.shape}")
    print(f"Output shape: {result.shape}")
    print(f"\nResult columns: {list(result.columns)}")
    print(f"\nSample transformed row:")
    print(result.iloc[1][['borough', 'crash_datetime', 'persons_injured', 'vehicle_type_1', 'severity_level']])
    
    return result

if __name__ == "__main__":
    print("="*50)
    print("TESTING TRANSFORMATION MODULES")
    print("="*50)
    
    weather_result = test_weather_transform()
    collisions_result = test_collisions_transform()
    
    print("\n" + "="*50)
    print("✅ ALL TESTS PASSED!")
    print("="*50)