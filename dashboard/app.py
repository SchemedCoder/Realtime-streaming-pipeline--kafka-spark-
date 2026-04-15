import streamlit as st
from snowflake.snowpark import Session
import os
from dotenv import load_dotenv

# Load Credentials
load_dotenv()

# Snowflake Connection Function
def create_session():
    connection_parameters = {
        "account": os.getenv("SF_ACCOUNT"),
        "user": os.getenv("SF_USER"),
        "password": os.getenv("SF_PASSWORD"),
        "warehouse": os.getenv("SF_WAREHOUSE"),
        "database": os.getenv("SF_DATABASE"),
        "schema": "GOLD"
    }
    return Session.builder.configs(connection_parameters).create()

# Page Configuration
st.set_page_config(page_title="Global Weather Analytics", layout="wide")
st.title("🌍 Real-Time Weather & Humidity Analytics")
st.markdown("---")

# Sidebar - Filter & Search
st.sidebar.header("Control Panel")
city_search = st.sidebar.text_input("🔍 Search City", "")
st.sidebar.info("Data updates every 1 minute via Snowflake Dynamic Tables.")

# Fetch Data from Gold Layer
try:
    session = create_session()
    
    # Query
    query = """
        SELECT 
            city, 
            avg_temp, 
            avg_humidity, 
            time_window 
        FROM WEATHER_DB.GOLD.CITY_PERFORMANCE 
        QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY time_window DESC) = 1
    """
    
    df = session.sql(query).to_pandas()

    if not df.empty:
        # Top Metrics Row (Now 4 Columns)
        m1, m2, m3, m4 = st.columns(4)
        
        with m1:
            hottest = df.iloc[df['AVG_TEMP'].idxmax()]
            st.metric("🔥 Hottest", f"{hottest['CITY']}", f"{hottest['AVG_TEMP']:.1f}°C")
            
        with m2:
            # Humidity Metric
            humid = df.iloc[df['AVG_HUMIDITY'].idxmax()]
            st.metric("💧 Most Humid", f"{humid['CITY']}", f"{humid['AVG_HUMIDITY']:.0f}%")
            
        with m3:
            coldest = df.iloc[df['AVG_TEMP'].idxmin()]
            st.metric("❄️ Coldest", f"{coldest['CITY']}", f"{coldest['AVG_TEMP']:.1f}°C")
            
        with m4:
            st.metric("🏙️ Cities Tracked", len(df))

        st.markdown("###")

        # Main Visuals - Comparison Charts
        left_col, right_col = st.columns([2, 1])

        with left_col:
            st.subheader("Temperature vs Humidity Comparison")
            # Display both metrics in one bar chart for better insight
            st.bar_chart(df, x="CITY", y=["AVG_TEMP", "AVG_HUMIDITY"])

        with right_col:
            st.subheader("Live Search Results")
            if city_search:
                filtered_df = df[df['CITY'].str.contains(city_search, case=False)]
                st.dataframe(filtered_df[['CITY', 'AVG_TEMP', 'AVG_HUMIDITY']], use_container_width=True, hide_index=True)
            else:
                st.dataframe(df[['CITY', 'AVG_TEMP', 'AVG_HUMIDITY']].head(10), use_container_width=True, hide_index=True)

    session.close()

except Exception as e:
    st.error(f"⚠️ Connection Error: {e}")

# Sidebar Refresh
if st.sidebar.button("🔄 Sync with Snowflake"):
    st.rerun()
