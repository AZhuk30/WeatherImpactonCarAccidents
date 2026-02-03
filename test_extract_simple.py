"""
Simple test for extraction module
"""
import sys
sys.path.append('.')

print("🧪 Testing extraction module imports...")

try:
    # Test if we can import the modules
    from src.config import BOROUGHS, WEATHER_PARAMS
    print("✅ Config imports work!")
    
    from src.extract import WeatherExtractor, CollisionsExtractor
    print("✅ Extract imports work!")
    
    print(f"\n📊 Config loaded:")
    print(f"  - Boroughs: {list(BOROUGHS.keys())}")
    print(f"  - Weather params: {WEATHER_PARAMS}")
    
    # Create instances
    print("\n🧱 Creating extractor instances...")
    weather_extractor = WeatherExtractor()
    collisions_extractor = CollisionsExtractor()
    
    print("✅ Extractors created successfully!")
    
    print("\n" + "="*50)
    print("✅ ALL IMPORT TESTS PASSED!")
    print("="*50)
    
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("\n💡 Make sure you have these packages installed:")
    print("pip install pandas requests openmeteo-requests requests-cache retry-requests")
except Exception as e:
    print(f"❌ Error: {e}")