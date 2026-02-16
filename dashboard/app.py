"""
Streamlit Dashboard for NYC Traffic Safety Analysis - Enhanced with Time Analysis
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

# Load the data from master files
@st.cache_data
def load_latest_data():
    """Load the master CSV files"""
    
    weather_path = "data/processed/weather_master.csv"
    collisions_path = "data/processed/collisions_master.csv"
    
    if not os.path.exists(weather_path) or not os.path.exists(collisions_path):
        return None, None
    
    try:
        weather_df = pd.read_csv(weather_path)
        collisions_df = pd.read_csv(collisions_path)
        
        # Convert datetime columns
        if 'datetime' in weather_df.columns:
            weather_df['datetime'] = pd.to_datetime(weather_df['datetime'], errors='coerce')
        
        if 'crash_datetime' in collisions_df.columns:
            collisions_df['crash_datetime'] = pd.to_datetime(collisions_df['crash_datetime'], errors='coerce')
        elif 'crash_date' in collisions_df.columns:
            collisions_df['crash_datetime'] = pd.to_datetime(collisions_df['crash_date'], errors='coerce')
        
        # Add time-based features if not present
        if 'crash_datetime' in collisions_df.columns:
            collisions_df['date'] = collisions_df['crash_datetime'].dt.date
            collisions_df['hour'] = collisions_df['crash_datetime'].dt.hour
            collisions_df['day_of_week'] = collisions_df['crash_datetime'].dt.day_name()
            collisions_df['month'] = collisions_df['crash_datetime'].dt.month
            collisions_df['month_name'] = collisions_df['crash_datetime'].dt.strftime('%B')
            collisions_df['is_weekend'] = collisions_df['crash_datetime'].dt.dayofweek >= 5
            collisions_df['is_rush_hour'] = collisions_df['hour'].isin([7, 8, 9, 16, 17, 18, 19])
        
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
            avg_daily = len(filtered_collisions) / filtered_collisions['date'].nunique() if 'date' in filtered_collisions.columns else 0
            st.metric("Avg Daily Collisions", f"{avg_daily:.1f}")
    
    # ========== VISUALIZATIONS ==========
    st.header("📈 Analysis Visualizations")
    
    tab1, tab2, tab3, tab4 = st.tabs(["⏰ Time Analysis", "📍 Location Patterns", "🌤️ Weather Impact", "📋 Data Tables"])
    
    with tab1:
        st.subheader("⏰ Temporal Patterns")
        
        # Hourly Distribution
        col1, col2 = st.columns(2)
        
        with col1:
            if 'hour' in filtered_collisions.columns:
                hourly = filtered_collisions.groupby('hour').size().reset_index(name='collisions')
                
                fig = px.line(hourly, x='hour', y='collisions',
                            title="Collisions by Hour of Day",
                            markers=True,
                            labels={'hour': 'Hour (24h)', 'collisions': 'Number of Collisions'})
                
                # Add rush hour shading
                fig.add_vrect(x0=7, x1=10, fillcolor="yellow", opacity=0.2, 
                            annotation_text="Morning Rush", annotation_position="top left")
                fig.add_vrect(x0=16, x1=19, fillcolor="orange", opacity=0.2,
                            annotation_text="Evening Rush", annotation_position="top left")
                
                fig.update_xaxes(tickmode='linear', tick0=0, dtick=2)
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            if 'is_rush_hour' in filtered_collisions.columns:
                rush_hour_data = filtered_collisions.groupby('is_rush_hour').agg({
                    'collision_id': 'count'
                }).reset_index()
                rush_hour_data['is_rush_hour'] = rush_hour_data['is_rush_hour'].map({
                    True: 'Rush Hour', False: 'Non-Rush Hour'
                })
                
                fig = px.pie(rush_hour_data, values='collision_id', names='is_rush_hour',
                           title="Rush Hour vs Non-Rush Hour",
                           color_discrete_sequence=['#ff7f0e', '#1f77b4'])
                st.plotly_chart(fig, use_container_width=True)
        
        # Day of Week Analysis
        col1, col2 = st.columns(2)
        
        with col1:
            if 'day_of_week' in filtered_collisions.columns:
                # Order days properly
                day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
                dow = filtered_collisions.groupby('day_of_week').size().reset_index(name='collisions')
                dow['day_of_week'] = pd.Categorical(dow['day_of_week'], categories=day_order, ordered=True)
                dow = dow.sort_values('day_of_week')
                
                fig = px.bar(dow, x='day_of_week', y='collisions',
                           title="Collisions by Day of Week",
                           color='collisions',
                           color_continuous_scale='blues',
                           labels={'day_of_week': 'Day', 'collisions': 'Number of Collisions'})
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            if 'is_weekend' in filtered_collisions.columns:
                weekend_data = filtered_collisions.groupby('is_weekend').size().reset_index(name='collisions')
                weekend_data['is_weekend'] = weekend_data['is_weekend'].map({
                    True: 'Weekend', False: 'Weekday'
                })
                
                fig = px.bar(weekend_data, x='is_weekend', y='collisions',
                           title="Weekday vs Weekend Collisions",
                           color='is_weekend',
                           color_discrete_map={'Weekday': '#2ecc71', 'Weekend': '#e74c3c'},
                           labels={'is_weekend': '', 'collisions': 'Number of Collisions'})
                st.plotly_chart(fig, use_container_width=True)
        
        # Monthly Trends
        if 'month_name' in filtered_collisions.columns:
            month_order = ['January', 'February', 'March', 'April', 'May', 'June',
                          'July', 'August', 'September', 'October', 'November', 'December']
            monthly = filtered_collisions.groupby('month_name').size().reset_index(name='collisions')
            monthly['month_name'] = pd.Categorical(monthly['month_name'], categories=month_order, ordered=True)
            monthly = monthly.sort_values('month_name')
            
            fig = px.bar(monthly, x='month_name', y='collisions',
                       title="Collisions by Month",
                       color='collisions',
                       color_continuous_scale='reds',
                       labels={'month_name': 'Month', 'collisions': 'Number of Collisions'})
            st.plotly_chart(fig, use_container_width=True)
        
        # Time-based heatmap
        if 'hour' in filtered_collisions.columns and 'day_of_week' in filtered_collisions.columns:
            st.subheader("📅 Collision Heatmap: Day x Hour")
            
            heatmap_data = filtered_collisions.groupby(['day_of_week', 'hour']).size().reset_index(name='collisions')
            heatmap_pivot = heatmap_data.pivot(index='day_of_week', columns='hour', values='collisions').fillna(0)
            
            # Reorder days
            day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            heatmap_pivot = heatmap_pivot.reindex([day for day in day_order if day in heatmap_pivot.index])
            
            fig = px.imshow(heatmap_pivot,
                           labels=dict(x="Hour of Day", y="Day of Week", color="Collisions"),
                           x=heatmap_pivot.columns,
                           y=heatmap_pivot.index,
                           color_continuous_scale="Reds",
                           title="Collision Intensity Heatmap")
            st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        st.subheader("📍 Location-Based Analysis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if 'borough' in filtered_collisions.columns:
                borough_counts = filtered_collisions['borough'].value_counts().reset_index()
                borough_counts.columns = ['borough', 'count']
                
                fig = px.bar(borough_counts, x='borough', y='count',
                           title="Collisions by Borough",
                           color='count',
                           color_continuous_scale='reds',
                           labels={'borough': 'Borough', 'count': 'Number of Collisions'})
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            if 'severity_level' in filtered_collisions.columns:
                severity_counts = filtered_collisions['severity_level'].value_counts().reset_index()
                severity_counts.columns = ['severity', 'count']
                
                fig = px.pie(severity_counts, values='count', names='severity',
                           title="Collision Severity Distribution",
                           hole=0.4,
                           color_discrete_sequence=px.colors.sequential.RdYlGn_r)
                st.plotly_chart(fig, use_container_width=True)
    
    with tab3:
        st.subheader("🌤️ Weather Impact Analysis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if 'weather_category' in weather_df.columns and len(weather_df) > 0:
                weather_counts = weather_df['weather_category'].value_counts().reset_index()
                weather_counts.columns = ['weather', 'count']
                
                fig = px.bar(weather_counts, x='weather', y='count',
                           title="Weather Conditions Distribution",
                           color='count',
                           color_continuous_scale='blues',
                           labels={'weather': 'Weather Condition', 'count': 'Frequency'})
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            if 'temperature_2m' in weather_df.columns:
                fig = px.histogram(weather_df, x='temperature_2m',
                                 title="Temperature Distribution (°C)",
                                 nbins=25,
                                 color_discrete_sequence=['orange'],
                                 labels={'temperature_2m': 'Temperature (°C)', 'count': 'Frequency'})
                st.plotly_chart(fig, use_container_width=True)
        
        # Weather vs Collisions (if we can match them)
        if 'weather_category' in weather_df.columns and 'date' in filtered_collisions.columns:
            st.subheader("🔄 Weather-Collision Correlation")
            
            # Aggregate by date and weather
            if 'datetime' in weather_df.columns:
                weather_df['date'] = weather_df['datetime'].dt.date
                weather_daily = weather_df.groupby(['date', 'weather_category']).size().reset_index(name='weather_count')
                collisions_daily = filtered_collisions.groupby('date').size().reset_index(name='collision_count')
                
                # Get most common weather per day
                weather_mode = weather_df.groupby('date')['weather_category'].agg(lambda x: x.mode()[0] if len(x.mode()) > 0 else 'CLEAR').reset_index()
                
                combined = collisions_daily.merge(weather_mode, on='date', how='left')
                weather_collision_summary = combined.groupby('weather_category')['collision_count'].mean().reset_index()
                weather_collision_summary.columns = ['Weather', 'Avg Daily Collisions']
                
                fig = px.bar(weather_collision_summary, x='Weather', y='Avg Daily Collisions',
                           title="Average Daily Collisions by Weather Condition",
                           color='Avg Daily Collisions',
                           color_continuous_scale='reds')
                st.plotly_chart(fig, use_container_width=True)
    
    with tab4:
        st.subheader("📋 Data Preview")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**Weather Data**")
            st.dataframe(filtered_weather.head(10), use_container_width=True)
        
        with col2:
            st.write("**Collisions Data**")
            st.dataframe(filtered_collisions.head(10), use_container_width=True)
    
    # ========== KEY INSIGHTS ==========
    st.header("💡 Key Insights")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.info("""
        **Temporal Patterns:**
        - Peak collision hours align with rush hour traffic
        - Weekend patterns differ from weekday patterns
        - Certain months show higher collision rates
        """)
        
        if 'hour' in filtered_collisions.columns:
            peak_hour = filtered_collisions['hour'].mode()[0]
            st.success(f"🕐 **Peak Hour:** {peak_hour}:00 - {peak_hour+1}:00")
    
    with col2:
        st.info("""
        **Weather Impact:**
        - Weather conditions correlate with collision frequency
        - Temperature and visibility affect driving safety
        - Adverse weather requires extra caution
        """)
        
        if 'severity_level' in filtered_collisions.columns:
            fatal_pct = (filtered_collisions['severity_level'] == 'FATAL').sum() / len(filtered_collisions) * 100
            st.warning(f"⚠️ **Fatal Collision Rate:** {fatal_pct:.2f}%")
    
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
    
    **Next steps:**
    1. Wait for the next automated update (runs daily at 2 AM UTC)
    2. Or manually trigger the workflow from the Actions tab
    """)

# Footer
st.markdown("---")
st.markdown("""
**NYC Traffic Safety Analysis** | Data Sources: NYC Open Data, Open-Meteo API  
*Updated automatically via GitHub Actions • Dashboard by Streamlit*
""")
