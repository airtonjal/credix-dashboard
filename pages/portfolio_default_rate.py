import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account

# Create BigQuery client
@st.cache_resource
def get_bigquery_client():
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )
    return bigquery.Client(credentials=credentials)

@st.cache_data(ttl="1h")
def load_default_rate_analysis():
    client = get_bigquery_client()
    query = """
    WITH default_metrics AS (
        SELECT
            snapshot_date,
            cohort_month,
            COUNT(DISTINCT asset_id) as total_loans,
            COUNT(DISTINCT CASE WHEN is_default = 1 THEN asset_id END) as defaulted_loans,
            SUM(total_expected_amount) as total_portfolio_value,
            SUM(CASE WHEN is_default = 1 THEN total_expected_amount END) as defaulted_value,
            AVG(CASE WHEN is_default = 1 THEN max_days_late END) as avg_days_to_default
        FROM `credix-analytics.gold.fact_payment_performance`
        GROUP BY snapshot_date, cohort_month
    )
    SELECT
        snapshot_date,
        cohort_month,
        total_loans,
        defaulted_loans,
        total_portfolio_value,
        defaulted_value,
        avg_days_to_default,
        SAFE_DIVIDE(defaulted_loans, total_loans) as default_rate,
        SAFE_DIVIDE(defaulted_value, total_portfolio_value) as default_rate_by_value
    FROM default_metrics
    ORDER BY snapshot_date DESC, cohort_month DESC
    """
    return pd.read_gbq(query, credentials=client.credentials)

st.title("Portfolio Default Rate Analysis")

try:
    # Load data
    df = load_default_rate_analysis()
    
    # KPI metrics row
    col1, col2, col3 = st.columns(3)
    
    with col1:
        latest_default_rate = df.iloc[0]['default_rate'] * 100
        st.metric("Current Default Rate", f"{latest_default_rate:.2f}%")
        
    with col2:
        latest_value_default = df.iloc[0]['default_rate_by_value'] * 100
        st.metric("Default Rate by Value", f"{latest_value_default:.2f}%")
        
    with col3:
        avg_days = df.iloc[0]['avg_days_to_default']
        st.metric("Avg Days to Default", f"{avg_days:.0f} days")

    # Default Rate Trend
    st.subheader("Default Rate Trend")
    fig_trend = go.Figure()
    
    fig_trend.add_trace(
        go.Scatter(
            x=df['snapshot_date'],
            y=df['default_rate'] * 100,
            name='Default Rate (%)',
            line=dict(color='red')
        )
    )
    
    fig_trend.add_trace(
        go.Scatter(
            x=df['snapshot_date'],
            y=df['default_rate_by_value'] * 100,
            name='Default Rate by Value (%)',
            line=dict(color='orange')
        )
    )
    
    fig_trend.update_layout(
        title="Default Rate Over Time",
        xaxis_title="Date",
        yaxis_title="Default Rate (%)",
        hovermode='x unified'
    )
    st.plotly_chart(fig_trend, use_container_width=True)

    # Cohort Analysis
    st.subheader("Default Rate by Cohort")
    cohort_matrix = df.pivot(
        index='snapshot_date',
        columns='cohort_month',
        values='default_rate'
    ) * 100

    fig_cohort = px.imshow(
        cohort_matrix,
        title="Default Rate by Cohort (%)",
        labels=dict(x="Cohort Month", y="Snapshot Date", color="Default Rate (%)"),
        color_continuous_scale="Reds",
        aspect="auto"
    )
    st.plotly_chart(fig_cohort, use_container_width=True)

except Exception as e:
    st.error(f"An error occurred: {str(e)}")
    st.error("Please make sure you have set up the correct credentials in your secrets.toml file.") 