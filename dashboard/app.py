"""
Streamlit Dashboard for NYC Traffic Safety Analysis
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import os

# Page configuration
st.set_page_config(
    page_title="NYC Traffic Safety - Weather Impact",
    page_icon="🚗",
    layout="wide"
)

# Title
st.title("🚗 NYC Traffic Safety - Weather Impact Analysis")
st.markdown("Analyzing the relationship between weather conditions and vehicle collisions in NYC")

# Load the data from master files (not glob pattern)
@st.cache_data
def load_latest_data():
    """Load the master CSV files"""
    
    # Direct paths to master files
    weather_path = "data/processed/weather_master.csv"
    collisions_path = "data/processed/collisions_master.csv"
    
    # Check if files exist
    if not os.path.exists(weather_path):
        st.error(f"❌ Weather file not found at: {weather_path}")
        st.info(f"Looking in: {os.getcwd()}")
        st.info(f"Files available: {os.listdir('data/processed') if os.path.exists('data/processed') else 'No data/processed directory'}")
        return None, None
    
    if not os.path.exists(collisions_path):
        st.error(f"❌ Collisions file not found at: {collisions_path}")
        return None, None
    
    try:
        # Load data
        weather_df = pd.read_csv(weather_path)
        collisions_df = pd.read_csv(collisions_path)
        
        # Convert datetime columns
        if 'datetime' in weather_df.columns:
            weather_df['datetime'] = pd.to_datetime(weather_df['datetime'], errors='coerce')
        
        if 'crash_datetime' in collisions_df.columns:
            collisions_df['crash_datetime'] = pd.to_datetime(collisions_df['crash_datetime'], errors='coerce')
        elif 'crash_date' in collisions_df.columns:
            collisions_df['crash_datetime'] = pd.to_datetime(collisions_df['crash_date'], errors='coerce')
        
        return weather_df, collisions_df
        
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return None, None

# Load data
weather_df, collisions_df = load_latest_data()

if weather_df is not None and collisions_df is not None:
    # Sidebar filters
    st.sidebar.header("🔍 Filters")
    
    # Borough filter
    if 'borough' in collisions_df.columns:
        boroughs = ['ALL'] + sorted(collisions_df['borough'].dropna().unique().tolist())
        selected_borough = st.sidebar.selectbox("Select Borough", boroughs)
    else:
        selected_borough = 'ALL'
    
    # Date range filter
    if 'crash_datetime' in collisions_df.columns:
        min_date = collisions_df['crash_datetime'].min()
        max_date = collisions_df['crash_datetime'].max()
        
        if pd.notna(min_date) and pd.notna(max_date):
            date_range = st.sidebar.date_input(
                "Date Range",
                value=(min_date, max_date),
                min_value=min_date,
                max_value=max_date
            )
    
    # Weather condition filter
    if 'weather_category' in weather_df.columns:
        weather_conditions = ['ALL'] + sorted(weather_df['weather_category'].dropna().unique().tolist())
        selected_weather = st.sidebar.selectbox("Weather Condition", weather_conditions)
    
    # Apply filters
    filtered_collisions = collisions_df.copy()
    filtered_weather = weather_df.copy()
    
    if selected_borough != 'ALL' and 'borough' in filtered_collisions.columns:
        filtered_collisions = filtered_collisions[filtered_collisions['borough'] == selected_borough]
        if 'borough' in filtered_weather.columns:
            filtered_weather = filtered_weather[filtered_weather['borough'] == selected_borough]
    
    if 'selected_weather' in locals() and selected_weather != 'ALL' and 'weather_category' in filtered_weather.columns:
        filtered_weather = filtered_weather[filtered_weather['weather_category'] == selected_weather]
    
    # ========== DASHBOARD METRICS ==========
    st.header("📊 Key Metrics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_collisions = len(filtered_collisions)
        st.metric("Total Collisions", f"{total_collisions:,}")
    
    with col2:
        total_injuries = filtered_collisions['persons_injured'].sum() if 'persons_injured' in filtered_collisions.columns else \
                        filtered_collisions['number_of_persons_injured'].sum() if 'number_of_persons_injured' in filtered_collisions.columns else 0
        st.metric("Total Injuries", f"{int(total_injuries):,}")
    
    with col3:
        total_fatalities = filtered_collisions['persons_killed'].sum() if 'persons_killed' in filtered_collisions.columns else \
                          filtered_collisions['number_of_persons_killed'].sum() if 'number_of_persons_killed' in filtered_collisions.columns else 0
        st.metric("Total Fatalities", f"{int(total_fatalities):,}")
    
    with col4:
        if 'severity_level' in filtered_collisions.columns:
            severe_collisions = len(filtered_collisions[filtered_collisions['severity_level'].isin(['SEVERE', 'FATAL'])])
            st.metric("Severe Collisions", f"{severe_collisions:,}")
        else:
            st.metric("Data Quality", "✅ Good")
    
    # ========== VISUALIZATIONS ==========
    st.header("📈 Analysis Visualizations")
    
    tab1, tab2, tab3 = st.tabs(["Collision Patterns", "Weather Impact", "Data Tables"])
    
    with tab1:
        col1, col2 = st.columns(2)
        
        with col1:
            if 'borough' in filtered_collisions.columns:
                borough_counts = filtered_collisions['borough'].value_counts().reset_index()
                borough_counts.columns = ['borough', 'count']
                
                fig = px.bar(borough_counts, x='borough', y='count',
                           title="Collisions by Borough",
                           color='count',
                           color_continuous_scale='reds')
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            if 'severity_level' in filtered_collisions.columns:
                severity_counts = filtered_collisions['severity_level'].value_counts().reset_index()
                severity_counts.columns = ['severity', 'count']
                
                fig = px.pie(severity_counts, values='count', names='severity',
                           title="Collision Severity Distribution",
                           hole=0.3)
                st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        col1, col2 = st.columns(2)
        
        with col1:
            if 'weather_category' in weather_df.columns and len(weather_df) > 0:
                weather_counts = weather_df['weather_category'].value_counts().reset_index()
                weather_counts.columns = ['weather', 'count']
                
                fig = px.bar(weather_counts, x='weather', y='count',
                           title="Weather Conditions Distribution",
                           color='count',
                           color_continuous_scale='blues')
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            if 'temperature_2m' in weather_df.columns:
                fig = px.histogram(weather_df, x='temperature_2m',
                                 title="Temperature Distribution",
                                 nbins=20,
                                 color_discrete_sequence=['orange'])
                st.plotly_chart(fig, use_container_width=True)
    
    with tab3:
        st.subheader("📋 Data Preview")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**Weather Data**")
            st.dataframe(filtered_weather.head(10), use_container_width=True)
        
        with col2:
            st.write("**Collisions Data**")
            st.dataframe(filtered_collisions.head(10), use_container_width=True)
    
    # ========== DATA DOWNLOAD ==========
    st.header("📥 Export Data")
    
    col1, col2 = st.columns(2)
    
    with col1:
        weather_csv = filtered_weather.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Download Weather Data (CSV)",
            data=weather_csv,
            file_name="nyc_weather_analysis.csv",
            mime="text/csv"
        )
    
    with col2:
        collisions_csv = filtered_collisions.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Download Collisions Data (CSV)",
            data=collisions_csv,
            file_name="nyc_collisions_analysis.csv",
            mime="text/csv"
        )

else:
    st.warning("⚠️ No data available")
    st.info("""
    The data files should be automatically updated by GitHub Actions.
    
    If you're seeing this message, the data files may not have been committed to the repository yet.
    
    **Next steps:**
    1. Check that files exist in: `data/processed/weather_master.csv` and `data/processed/collisions_master.csv`
    2. Wait for the next automated update (runs daily at 2 AM UTC)
    3. Or manually trigger the workflow from the Actions tab
    """)

# Footer
st.markdown("---")
st.markdown("""
**NYC Traffic Safety Analysis** | Data Sources: NYC Open Data, Open-Meteo API  
*Updated automatically via GitHub Actions*
""")
