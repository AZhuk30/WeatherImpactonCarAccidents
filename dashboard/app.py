"""
Minimal NYC Traffic Safety Dashboard - Test Version
"""
import streamlit as st
import pandas as pd
import os

st.set_page_config(
    page_title="NYC Traffic Safety",
    page_icon="🚗",
    layout="wide"
)

st.title("🚗 NYC Traffic Safety Dashboard")
st.markdown("Testing deployment...")

# Check if data files exist
st.header("📊 Data Status")

weather_path = "data/processed/weather_master.csv"
collision_path = "data/processed/collisions_master.csv"

col1, col2 = st.columns(2)

with col1:
    if os.path.exists(weather_path):
        try:
            weather_df = pd.read_csv(weather_path)
            st.success(f"✅ Weather data loaded: {len(weather_df):,} records")
            st.dataframe(weather_df.head())
        except Exception as e:
            st.error(f"❌ Error loading weather data: {e}")
    else:
        st.warning(f"⚠️ Weather file not found at: {weather_path}")
        st.info("Available files:")
        st.code(os.listdir("data/processed") if os.path.exists("data/processed") else "No data/processed directory")

with col2:
    if os.path.exists(collision_path):
        try:
            collision_df = pd.read_csv(collision_path)
            st.success(f"✅ Collision data loaded: {len(collision_df):,} records")
            st.dataframe(collision_df.head())
        except Exception as e:
            st.error(f"❌ Error loading collision data: {e}")
    else:
        st.warning(f"⚠️ Collision file not found at: {collision_path}")

st.markdown("---")
st.markdown("**If you see this page, Streamlit deployment is working!** ✨")
