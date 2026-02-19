"""
Streamlit Dashboard — NYC Traffic Safety Analysis
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st
import pandas as pd
import plotly.express as px

from src.config import PROCESSED_DATA_DIR
from src.transform import make_tz_naive, top_contributing_factors


st.set_page_config(
    page_title="NYC Traffic Safety - Weather Impact",
    page_icon="🚗",
    layout="wide",
)

st.title("🚗 NYC Traffic Safety — Weather Impact Analysis")
st.markdown("Analysing the relationship between weather conditions and vehicle collisions in NYC")


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

@st.cache_data(ttl=3600)
def load_data() -> tuple[pd.DataFrame | None, pd.DataFrame | None]:
    weather_path    = PROCESSED_DATA_DIR / "weather_master.csv"
    collisions_path = PROCESSED_DATA_DIR / "collisions_master.csv"

    missing = [p for p in (weather_path, collisions_path) if not p.exists()]
    if missing:
        for p in missing:
            st.error(f"❌ File not found: {p}")
        st.info(
            "Data files are created by the ETL pipeline. Run `python run_pipeline.py` "
            "locally or wait for the next GitHub Actions update (daily at 2 AM UTC)."
        )
        return None, None

    try:
        weather_df    = pd.read_csv(weather_path)
        collisions_df = pd.read_csv(collisions_path)

        if "datetime" in weather_df.columns:
            weather_df["datetime"] = make_tz_naive(weather_df["datetime"])
            weather_df["_date"]    = weather_df["datetime"].dt.date

        if "crash_datetime" in collisions_df.columns:
            collisions_df["crash_datetime"] = make_tz_naive(collisions_df["crash_datetime"])
        elif "crash_date" in collisions_df.columns:
            collisions_df["crash_datetime"] = make_tz_naive(collisions_df["crash_date"])

        if "crash_datetime" in collisions_df.columns:
            collisions_df["_date"] = collisions_df["crash_datetime"].dt.date

        return weather_df, collisions_df

    except Exception as e:
        st.error(f"Error loading data: {e}")
        return None, None


# ---------------------------------------------------------------------------
# Sidebar filters
# ---------------------------------------------------------------------------

def apply_filters(
    weather_df: pd.DataFrame,
    collisions_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:

    st.sidebar.header("🔍 Filters")

    # Borough
    boroughs = ["ALL"]
    if "borough" in collisions_df.columns:
        boroughs += sorted(collisions_df["borough"].dropna().unique().tolist())
    selected_borough = st.sidebar.selectbox("Borough", boroughs)

    # Date range
    date_range = None
    if "crash_datetime" in collisions_df.columns and collisions_df["crash_datetime"].notna().any():
        min_d = collisions_df["crash_datetime"].min().date()
        max_d = collisions_df["crash_datetime"].max().date()
        date_range = st.sidebar.date_input("Date Range", value=(min_d, max_d), min_value=min_d, max_value=max_d)

    # Weather condition
    selected_weather = "ALL"
    if "weather_category" in weather_df.columns:
        conditions = ["ALL"] + sorted(weather_df["weather_category"].dropna().unique().tolist())
        selected_weather = st.sidebar.selectbox("Weather Condition", conditions)

    # Apply
    fc = collisions_df.copy()
    fw = weather_df.copy()

    if selected_borough != "ALL":
        if "borough" in fc.columns:
            fc = fc[fc["borough"] == selected_borough]
        if "borough" in fw.columns:
            fw = fw[fw["borough"] == selected_borough]

    if date_range and isinstance(date_range, tuple) and len(date_range) == 2:
        start_d, end_d = date_range
        if "_date" in fc.columns:
            fc = fc[(fc["_date"] >= start_d) & (fc["_date"] <= end_d)]
        if "_date" in fw.columns:
            fw = fw[(fw["_date"] >= start_d) & (fw["_date"] <= end_d)]

    if selected_weather != "ALL" and "weather_category" in fw.columns:
        fw = fw[fw["weather_category"] == selected_weather]
        if "weather_category" in fc.columns:
            fc = fc[fc["weather_category"] == selected_weather]

    return fw, fc


# ---------------------------------------------------------------------------
# Metric helpers — columns are guaranteed by transform.py
# ---------------------------------------------------------------------------

def _sum(df: pd.DataFrame, col: str) -> int:
    return int(df[col].sum()) if col in df.columns else 0


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

weather_df, collisions_df = load_data()

if weather_df is None or collisions_df is None:
    st.stop()

filtered_weather, filtered_collisions = apply_filters(weather_df, collisions_df)

# ── Key metrics ────────────────────────────────────────────────────────────
st.header("📊 Key Metrics")
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Total Collisions", f"{len(filtered_collisions):,}")
with col2:
    st.metric("Total Injuries", f"{_sum(filtered_collisions, 'persons_injured'):,}")
with col3:
    st.metric("Total Fatalities", f"{_sum(filtered_collisions, 'persons_killed'):,}")
with col4:
    if "severity_level" in filtered_collisions.columns:
        severe = filtered_collisions["severity_level"].isin(["SEVERE", "FATAL"]).sum()
        st.metric("Severe / Fatal", f"{severe:,}")
    else:
        st.metric("Severe / Fatal", "N/A")

# ── Tabs ───────────────────────────────────────────────────────────────────
tab1, tab2, tab3 = st.tabs(["Collision Patterns", "Weather Impact", "Data Tables"])

# ── Tab 1: Collision Patterns ──────────────────────────────────────────────
with tab1:
    col1, col2 = st.columns(2)

    with col1:
        if "borough" in filtered_collisions.columns and len(filtered_collisions):
            counts = filtered_collisions["borough"].value_counts().reset_index()
            counts.columns = ["borough", "count"]
            st.plotly_chart(
                px.bar(counts, x="borough", y="count", title="Collisions by Borough",
                       color="count", color_continuous_scale="reds"),
                use_container_width=True,
            )
        else:
            st.info("No borough data available.")

    with col2:
        if "severity_level" in filtered_collisions.columns and len(filtered_collisions):
            sev = filtered_collisions["severity_level"].value_counts().reset_index()
            sev.columns = ["severity", "count"]
            st.plotly_chart(
                px.pie(sev, values="count", names="severity",
                       title="Collision Severity Distribution", hole=0.3),
                use_container_width=True,
            )
        else:
            st.info("No severity data available.")

    st.subheader("⚠️ Top Contributing Factors")
    top_factors = top_contributing_factors(filtered_collisions)

    if top_factors.empty:
        st.info("No contributing factor data found.")
    else:
        c1, c2 = st.columns([2, 1])
        with c1:
            fig = px.bar(top_factors, x="factor", y="count", title="Top Contributing Factors")
            fig.update_layout(xaxis_title="Factor", yaxis_title="Count")
            st.plotly_chart(fig, use_container_width=True)
        with c2:
            st.dataframe(top_factors, use_container_width=True, hide_index=True)

# ── Tab 2: Weather Impact ──────────────────────────────────────────────────
with tab2:
    col1, col2 = st.columns(2)

    with col1:
        if "weather_category" in filtered_weather.columns and len(filtered_weather):
            wc = filtered_weather["weather_category"].value_counts().reset_index()
            wc.columns = ["weather", "count"]
            st.plotly_chart(
                px.bar(wc, x="weather", y="count", title="Weather Conditions Distribution",
                       color="count", color_continuous_scale="blues"),
                use_container_width=True,
            )
        else:
            st.info("No weather category data available.")

    with col2:
        if "temperature_2m" in filtered_weather.columns and len(filtered_weather):
            st.plotly_chart(
                px.histogram(filtered_weather, x="temperature_2m",
                             title="Temperature Distribution (°C)", nbins=20),
                use_container_width=True,
            )
        else:
            st.info("No temperature data available.")

    # Severe collision rate by weather — requires merging on borough + hour
    st.subheader("🚨 Severe Collision Rate (%) by Weather")

    need_coll = {"borough", "crash_datetime", "severity_level"}
    need_wea  = {"borough", "datetime", "weather_category"}

    if need_coll.issubset(filtered_collisions.columns) and need_wea.issubset(filtered_weather.columns):
        tmp_c = filtered_collisions.copy()
        tmp_w = filtered_weather.copy()

        # Normalise borough before merging to prevent silent empty joins
        tmp_c["borough"] = tmp_c["borough"].str.upper().str.strip()
        tmp_w["borough"] = tmp_w["borough"].str.upper().str.strip()

        tmp_c["_hour"] = tmp_c["crash_datetime"].dt.floor("h")
        tmp_w["_hour"] = tmp_w["datetime"].dt.floor("h")

        merged = pd.merge(
            tmp_c,
            tmp_w[["borough", "_hour", "weather_category"]],
            left_on=["borough", "_hour"],
            right_on=["borough", "_hour"],
            how="inner",
        )

        if merged.empty:
            st.warning(
                "No collisions matched to weather records. "
                "Check that borough names and hourly timestamps align across both datasets."
            )
        else:
            merged["is_severe"] = merged["severity_level"].isin(["SEVERE", "FATAL"])
            rate = (
                merged.groupby("weather_category")["is_severe"]
                .mean()
                .mul(100)
                .reset_index()
                .rename(columns={"is_severe": "severe_pct"})
                .sort_values("severe_pct", ascending=False)
            )
            fig = px.bar(rate, x="weather_category", y="severe_pct",
                         title="Severe Collision Rate (%) by Weather Category")
            fig.update_layout(xaxis_title="Weather", yaxis_title="Severe Rate (%)")
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info(
            "Missing required columns for weather-collision merge. "
            "Need: borough, crash_datetime, severity_level (collisions) "
            "and borough, datetime, weather_category (weather)."
        )

# ── Tab 3: Data Tables ─────────────────────────────────────────────────────
with tab3:
    st.subheader("📋 Data Preview")
    col1, col2 = st.columns(2)
    with col1:
        st.write("**Weather (first 10 rows)**")
        st.dataframe(filtered_weather.head(10), use_container_width=True)
    with col2:
        st.write("**Collisions (first 10 rows)**")
        st.dataframe(filtered_collisions.head(10), use_container_width=True)

# ── Export ─────────────────────────────────────────────────────────────────
st.header("📥 Export Data")
col1, col2 = st.columns(2)

with col1:
    st.download_button(
        label="Download Weather CSV",
        data=filtered_weather.to_csv(index=False).encode("utf-8"),
        file_name="nyc_weather_analysis.csv",
        mime="text/csv",
    )
with col2:
    st.download_button(
        label="Download Collisions CSV",
        data=filtered_collisions.to_csv(index=False).encode("utf-8"),
        file_name="nyc_collisions_analysis.csv",
        mime="text/csv",
    )

st.markdown("---")
st.markdown(
    "**NYC Traffic Safety Analysis** | Data: NYC Open Data + Open-Meteo  \n"
    "*Updated automatically via GitHub Actions*"
)