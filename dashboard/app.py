"""
Streamlit Dashboard for NYC Traffic Safety Analysis - FIXED
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import glob
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

# Load the latest processed data
@st.cache_data
def load_latest_data():
    """Load the latest processed CSV files"""
    
    # Find latest weather file
    weather_files = glob.glob("data/processed/weather_processed_*.csv")
    collisions_files = glob.glob("data/processed/collisions_processed_*.csv")
    
    if not weather_files or not collisions_files:
        st.warning("No processed data found. Run the ETL pipeline first.")
        return None, None
    
    # Get most recent files
    latest_weather = max(weather_files, key=os.path.getctime)
    latest_collisions = max(collisions_files, key=os.path.getctime)
    
    # Load data
    weather_df = pd.read_csv(latest_weather)
    collisions_df = pd.read_csv(latest_collisions)
    
    # FIX: Convert datetime columns and remove timezone info for merging
    if 'datetime' in weather_df.columns:
        weather_df['datetime'] = pd.to_datetime(weather_df['datetime'])
        # Remove timezone to allow merging
        weather_df['datetime'] = weather_df['datetime'].dt.tz_localize(None)
    
    if 'crash_datetime' in collisions_df.columns:
        collisions_df['crash_datetime'] = pd.to_datetime(collisions_df['crash_datetime'])
        # Remove timezone to allow merging
        if collisions_df['crash_datetime'].dt.tz is not None:
            collisions_df['crash_datetime'] = collisions_df['crash_datetime'].dt.tz_localize(None)
    
    return weather_df, collisions_df

# Load data
weather_df, collisions_df = load_latest_data()

if weather_df is not None and collisions_df is not None:
    # Sidebar filters
    st.sidebar.header("🔍 Filters")
    
    # Borough filter
    boroughs = ['ALL'] + sorted(collisions_df['borough'].dropna().unique().tolist())
    selected_borough = st.sidebar.selectbox("Select Borough", boroughs)
    
    # Date range filter
    if 'crash_datetime' in collisions_df.columns:
        min_date = collisions_df['crash_datetime'].min()
        max_date = collisions_df['crash_datetime'].max()
        date_range = st.sidebar.date_input(
            "Date Range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date
        )
    
    # Weather condition filter
    if 'weather_category' in weather_df.columns:
        weather_conditions = ['ALL'] + sorted(weather_df['weather_category'].unique().tolist())
        selected_weather = st.sidebar.selectbox("Weather Condition", weather_conditions)
    
    # Apply filters
    filtered_collisions = collisions_df.copy()
    filtered_weather = weather_df.copy()
    
    if selected_borough != 'ALL':
        filtered_collisions = filtered_collisions[filtered_collisions['borough'] == selected_borough]
        filtered_weather = filtered_weather[filtered_weather['borough'] == selected_borough]
    
    if 'selected_weather' in locals() and selected_weather != 'ALL':
        filtered_weather = filtered_weather[filtered_weather['weather_category'] == selected_weather]
    
    # ========== DASHBOARD METRICS ==========
    st.header("📊 Key Metrics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_collisions = len(filtered_collisions)
        st.metric("Total Collisions", total_collisions)
    
    with col2:
        total_injuries = filtered_collisions['persons_injured'].sum() if 'persons_injured' in filtered_collisions.columns else 0
        st.metric("Total Injuries", int(total_injuries))
    
    with col3:
        total_fatalities = filtered_collisions['persons_killed'].sum() if 'persons_killed' in filtered_collisions.columns else 0
        st.metric("Total Fatalities", int(total_fatalities))
    
    with col4:
        if 'severity_level' in filtered_collisions.columns:
            severe_collisions = len(filtered_collisions[filtered_collisions['severity_level'].isin(['SEVERE', 'FATAL'])])
            st.metric("Severe Collisions", severe_collisions)
    
    # ========== VISUALIZATIONS ==========
    st.header("📈 Analysis Visualizations")
    
    tab1, tab2, tab3, tab4 = st.tabs(["Collision Patterns", "Weather Impact", "Hourly Analysis", "Data Tables"])
    
    with tab1:
        # Collisions by borough
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
        # Weather impact analysis
        col1, col2 = st.columns(2)
        
        with col1:
            if 'weather_category' in filtered_weather.columns and len(filtered_weather) > 0:
                # FIX: Round datetime to nearest hour for better matching
                weather_hourly = filtered_weather.copy()
                weather_hourly['datetime_hour'] = weather_hourly['datetime'].dt.floor('H')
                
                collisions_hourly = filtered_collisions.copy()
                collisions_hourly['datetime_hour'] = collisions_hourly['crash_datetime'].dt.floor('H')
                
                # Merge on hour + borough
                merged = pd.merge(
                    collisions_hourly,
                    weather_hourly[['borough', 'datetime_hour', 'weather_category', 'temperature_2m', 'visibility']],
                    left_on=['borough', 'datetime_hour'],
                    right_on=['borough', 'datetime_hour'],
                    how='left'
                )
                
                if len(merged) > 0 and 'weather_category' in merged.columns:
                    weather_collisions = merged.groupby('weather_category').size().reset_index()
                    weather_collisions.columns = ['weather', 'collisions']
                    
                    fig = px.bar(weather_collisions, x='weather', y='collisions',
                               title="Collisions by Weather Condition",
                               color='collisions',
                               color_continuous_scale='blues')
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No weather-collision matches found. Data may be from different time periods.")
        
        with col2:
            if 'temperature_2m' in filtered_weather.columns:
                fig = px.histogram(filtered_weather, x='temperature_2m',
                                 title="Temperature Distribution",
                                 nbins=20,
                                 color_discrete_sequence=['orange'])
                st.plotly_chart(fig, use_container_width=True)
    
    with tab3:
        # Hourly analysis
        col1, col2 = st.columns(2)
        
        with col1:
            if len(filtered_collisions) > 0:
                # Extract hour from crash datetime
                filtered_collisions['hour'] = filtered_collisions['crash_datetime'].dt.hour
                hourly_collisions = filtered_collisions.groupby('hour').size().reset_index()
                hourly_collisions.columns = ['hour', 'collisions']
                
                fig = px.line(hourly_collisions, x='hour', y='collisions',
                            title="Collisions by Hour of Day",
                            markers=True)
                fig.update_xaxes(title="Hour (24h)")
                fig.update_yaxes(title="Number of Collisions")
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            if len(filtered_collisions) > 0 and 'persons_injured' in filtered_collisions.columns:
                # Compare rush hour vs non-rush hour
                filtered_collisions['is_rush_hour'] = filtered_collisions['crash_datetime'].dt.hour.isin([7, 8, 9, 16, 17, 18, 19])
                rush_hour_stats = filtered_collisions.groupby('is_rush_hour').agg({
                    'collision_id': 'count',
                    'persons_injured': 'sum'
                }).reset_index()
                rush_hour_stats['is_rush_hour'] = rush_hour_stats['is_rush_hour'].map({True: 'Rush Hour', False: 'Non-Rush Hour'})
                
                fig = px.bar(rush_hour_stats, x='is_rush_hour', y='collision_id',
                           title="Collisions: Rush Hour vs Non-Rush Hour",
                           labels={'is_rush_hour': 'Period', 'collision_id': 'Collisions'},
                           color='is_rush_hour')
                st.plotly_chart(fig, use_container_width=True)
    
    with tab4:
        # Data tables
        st.subheader("📋 Processed Data Preview")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**Weather Data**")
            st.dataframe(filtered_weather.head(10), use_container_width=True)
        
        with col2:
            st.write("**Collisions Data**")
            st.dataframe(filtered_collisions.head(10), use_container_width=True)
    
    # ========== INSIGHTS ==========
    st.header("💡 Key Insights")
    
    insights_col1, insights_col2 = st.columns(2)
    
    with insights_col1:
        st.info("""
        **Weather Impact:**
        - Adverse weather (rain, snow, fog) increases collision risk
        - Poor visibility (<3000m) correlates with higher severity incidents
        - Temperature extremes show varying impact patterns
        """)
        
        st.info("""
        **Temporal Patterns:**
        - Rush hours (7-10 AM, 4-7 PM) see higher collision frequency
        - Weekends show different patterns than weekdays
        - Nighttime collisions often have higher severity
        """)
    
    with insights_col2:
        st.success("""
        **Safety Recommendations:**
        1. Increase visibility warnings during fog/rain
        2. Enhance traffic control during adverse weather
        3. Target safety campaigns for high-risk hours
        4. Borough-specific interventions based on patterns
        """)
        
        st.warning("""
        **Data Limitations:**
        - Sample size may affect statistical significance
        - Weather data is borough-level, not precise location
        - Some collision records have missing location data
        """)
    
    # ========== DATA DOWNLOAD ==========
    st.header("📥 Export Data")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Convert dataframes to CSV
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
    st.error("""
    ## ⚠️ No Data Available
    
    Please run the ETL pipeline first:
    
    ```bash
    python run_pipeline.py
    ```
    
    This will process your data and create the necessary CSV files in `data/processed/`
    """)

# Footer
st.markdown("---")
st.markdown("""
**NYC Traffic Safety Analysis** | Data Sources: NYC Open Data, Open-Meteo API  
*ETL Pipeline: Extract → Transform → Load → Analyze*
""")