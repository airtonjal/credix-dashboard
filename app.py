import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account
import os
from dotenv import load_dotenv

# Page config
st.set_page_config(
    page_title="Credix Analytics Dashboard",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Create BigQuery credentials
@st.cache_resource
def get_credentials():
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )
    return credentials

# Cache data loading functions
@st.cache_data(ttl="1h")
def load_loan_performance():
    credentials = get_credentials()
    query = """
    SELECT 
        lp.*,
        b.buyer_tax_id as company_id,
        b.main_cnae as industry_sector,
        b.uf as state_code,
        b.company_size,
        b.risk_category
    FROM `credix-analytics.gold.fact_loan_performance` lp
    JOIN `credix-analytics.gold.dim_borrower` b ON lp.borrower_key = b.borrower_key
    """
    return pd.read_gbq(query, credentials=credentials, project_id=credentials.project_id)

@st.cache_data(ttl="1h")
def load_portfolio_risk():
    credentials = get_credentials()
    query = """
    SELECT 
        snapshot_date,
        default_rate,
        npl_ratio,
        avg_max_days_late,
        avg_installments_over_30d_late,
        fully_paid_on_time_amount,
        fully_paid_delayed_amount,
        overdue_amount,
        npl_amount,
        total_portfolio_value,
        late_within_30_count,
        total_payments
    FROM `credix-analytics.gold.fact_portfolio_risk`
    ORDER BY snapshot_date
    """
    return pd.read_gbq(query, credentials=credentials, project_id=credentials.project_id)

@st.cache_data(ttl="1h")
def load_payment_performance():
    credentials = get_credentials()
    query = """
    WITH latest_data AS (
        SELECT 
            asset_id,
            last_due_date,
            payment_status,
            total_original_amount,
            total_expected_amount,
            total_paid_amount,
            ROW_NUMBER() OVER (PARTITION BY asset_id ORDER BY last_due_date DESC) as rn
        FROM `credix-analytics.gold.fact_payment_performance`
    ),
    current_status AS (
        SELECT 
            payment_status,
            COUNT(*) as count,
            SUM(total_original_amount) as total_amount
        FROM latest_data
        WHERE rn = 1  -- Get only the latest record for each asset
        GROUP BY payment_status
    ),
    historical_stats AS (
        SELECT 
            DATE(last_due_date) as date,
            payment_status,
            COUNT(*) as count,
            SUM(total_original_amount) as total_amount
        FROM `credix-analytics.gold.fact_payment_performance`
        GROUP BY DATE(last_due_date), payment_status
    )
    SELECT 
        'CURRENT' as date,
        payment_status,
        count,
        total_amount,
        ROUND(total_amount / SUM(total_amount) OVER () * 100, 2) as percentage
    FROM current_status
    
    UNION ALL
    
    SELECT 
        CAST(date AS STRING),
        payment_status,
        count,
        total_amount,
        ROUND(total_amount / SUM(total_amount) OVER (PARTITION BY date) * 100, 2) as percentage
    FROM historical_stats
    ORDER BY date = 'CURRENT' DESC, date, payment_status
    """
    return pd.read_gbq(query, credentials=credentials, project_id=credentials.project_id)

# Sidebar
st.sidebar.title("Credix Analytics")
page = st.sidebar.radio(
    "Choose a Dashboard",
    ["Portfolio Overview", "Risk Analysis", "Payment Behavior", "Cohort Analysis"],
    index=0,  # Default to first option
    key="navigation"
)

try:
    # Load data
    if page == "Portfolio Overview":
        st.title("Portfolio Overview")
        
        # Load data
        df_loan = load_loan_performance()
        
        # KPI metrics row
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            total_loans = len(df_loan['asset_id'].unique())
            st.metric("Total Loans", f"{total_loans:,}")
            
        with col2:
            total_borrowers = len(df_loan['borrower_key'].unique())
            st.metric("Total Borrowers", f"{total_borrowers:,}")
            
        with col3:
            total_amount = df_loan['original_amount'].sum()
            st.metric("Total Loan Amount", f"R$ {total_amount:,.2f}")
            
        with col4:
            avg_loan = total_amount / total_loans
            st.metric("Average Loan Size", f"R$ {avg_loan:,.2f}")

        # Loan Status Distribution
        st.subheader("Loan Status Distribution")
        status_dist = df_loan['payment_status'].value_counts()
        fig_status = px.pie(
            values=status_dist.values,
            names=status_dist.index,
            title="Distribution of Loan Payment Status",
            color=status_dist.index,
            color_discrete_map={
                'FULLY_PAID_ON_TIME': '#2ecc71',     # Green
                'FULLY_PAID_WITH_DELAYS': '#e74c3c',  # Red
                'HAS_OVERDUE': '#f1c40f'             # Yellow
            }
        )
        st.plotly_chart(fig_status, use_container_width=True)

        # Industry Distribution
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Loans by Industry")
            industry_dist = df_loan.groupby('industry_sector')['original_amount'].sum().sort_values(ascending=True)
            fig_industry = px.bar(
                x=industry_dist.values,
                y=industry_dist.index,
                orientation='h',
                title="Total Loan Amount by Industry (CNAE)"
            )
            fig_industry.update_layout(yaxis_title="Industry (CNAE)", xaxis_title="Total Loan Amount (R$)")
            st.plotly_chart(fig_industry, use_container_width=True)
            
        with col2:
            st.subheader("Geographic Distribution")
            state_dist = df_loan.groupby('state_code')['original_amount'].sum().sort_values(ascending=False)
            fig_geo = px.bar(
                x=state_dist.index,
                y=state_dist.values,
                title="Total Loan Amount by State (UF)"
            )
            fig_geo.update_layout(xaxis_title="State (UF)", yaxis_title="Total Loan Amount (R$)")
            st.plotly_chart(fig_geo, use_container_width=True)

        # Add Company Size Distribution
        st.subheader("Company Size Distribution")
        size_dist = df_loan.groupby('company_size')['original_amount'].sum().sort_values(ascending=True)
        fig_size = px.bar(
            x=size_dist.values,
            y=size_dist.index,
            orientation='h',
            title="Total Loan Amount by Company Size"
        )
        fig_size.update_layout(yaxis_title="Company Size", xaxis_title="Total Loan Amount (R$)")
        st.plotly_chart(fig_size, use_container_width=True)

        # Add Risk Category Distribution
        st.subheader("Risk Category Distribution")
        risk_dist = df_loan.groupby('risk_category')['original_amount'].sum().sort_values(ascending=True)
        fig_risk = px.bar(
            x=risk_dist.values,
            y=risk_dist.index,
            orientation='h',
            title="Total Loan Amount by Risk Category"
        )
        fig_risk.update_layout(yaxis_title="Risk Category", xaxis_title="Total Loan Amount (R$)")
        st.plotly_chart(fig_risk, use_container_width=True)

    elif page == "Risk Analysis":
        # Remove both titles
        
        # Load data
        credentials = get_credentials()
        query = """
        WITH default_metrics AS (
            SELECT
                last_due_date as analysis_date,
                cohort_month,
                COUNT(DISTINCT asset_id) as total_loans,
                COUNT(DISTINCT CASE WHEN is_default = 1 THEN asset_id END) as defaulted_loans,
                SUM(total_expected_amount) as total_portfolio_value,
                SUM(CASE WHEN is_default = 1 THEN total_expected_amount END) as defaulted_value,
                AVG(CASE WHEN is_default = 1 THEN max_days_late END) as avg_days_to_default
            FROM `credix-analytics.gold.fact_payment_performance`
            WHERE last_due_date IS NOT NULL
            GROUP BY last_due_date, cohort_month
            HAVING total_loans > 0  -- Ensure we only get meaningful data points
        )
        SELECT
            analysis_date,
            cohort_month,
            total_loans,
            COALESCE(defaulted_loans, 0) as defaulted_loans,
            total_portfolio_value,
            COALESCE(defaulted_value, 0) as defaulted_value,
            COALESCE(avg_days_to_default, 0) as avg_days_to_default,
            COALESCE(SAFE_DIVIDE(defaulted_loans, total_loans), 0) as default_rate,
            COALESCE(SAFE_DIVIDE(defaulted_value, total_portfolio_value), 0) as default_rate_by_value
        FROM default_metrics
        ORDER BY analysis_date DESC, cohort_month DESC
        LIMIT 1000  -- Add reasonable limit to avoid processing too much data
        """
        df = pd.read_gbq(query, credentials=credentials, project_id=credentials.project_id)
        
        if df.empty:
            st.warning("No default rate data available.")
        else:
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
                    x=df['analysis_date'],
                    y=df['default_rate'] * 100,
                    name='Default Rate (%)',
                    line=dict(color='red')
                )
            )
            
            fig_trend.add_trace(
                go.Scatter(
                    x=df['analysis_date'],
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
                index='analysis_date',
                columns='cohort_month',
                values='default_rate'
            ) * 100

            fig_cohort = px.imshow(
                cohort_matrix,
                title="Default Rate by Cohort (%)",
                labels=dict(x="Cohort Month", y="Analysis Date", color="Default Rate (%)"),
                color_continuous_scale="Reds",
                aspect="auto"
            )
            st.plotly_chart(fig_cohort, use_container_width=True)

    elif page == "Payment Behavior":
        st.title("Payment Behavior Analysis")
        
        # Load data
        df_payment = load_payment_performance()
        
        # Split current status and historical data
        current_status = df_payment[df_payment['date'] == 'CURRENT']
        historical_data = df_payment[df_payment['date'] != 'CURRENT'].copy()
        historical_data['date'] = pd.to_datetime(historical_data['date']).dt.strftime('%Y-%m-%d')
        
        # Payment Status Timeline
        st.subheader("Payment Status Over Time")
        
        # Create two columns for different views
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Number of Loans by Status")
            fig_count = px.area(
                historical_data,
                x='date',
                y='count',
                color='payment_status',
                title="Number of Loans by Payment Status Over Time",
                color_discrete_map={
                    'FULLY_PAID_ON_TIME': '#2ecc71',     # Green
                    'FULLY_PAID_WITH_DELAYS': '#e74c3c',  # Red
                    'HAS_OVERDUE': '#f1c40f'             # Yellow
                }
            )
            fig_count.update_layout(
                xaxis_title="Date",
                yaxis_title="Number of Loans",
                hovermode='x unified',
                xaxis=dict(
                    type='category',
                    tickangle=-45,
                    nticks=10,
                    tickmode='auto',
                    showticklabels=True
                )
            )
            st.plotly_chart(fig_count, use_container_width=True)
        
        with col2:
            st.subheader("Portfolio Value by Status (%)")
            fig_amount = px.area(
                historical_data,
                x='date',
                y='percentage',
                color='payment_status',
                title="Portfolio Value Distribution by Payment Status (%)",
                color_discrete_map={
                    'FULLY_PAID_ON_TIME': '#2ecc71',     # Green
                    'FULLY_PAID_WITH_DELAYS': '#e74c3c',  # Red
                    'HAS_OVERDUE': '#f1c40f'             # Yellow
                }
            )
            fig_amount.update_layout(
                xaxis_title="Date",
                yaxis_title="Percentage of Portfolio Value (%)",
                hovermode='x unified',
                yaxis_range=[0, 100],
                xaxis=dict(
                    type='category',
                    tickangle=-45,
                    nticks=10,
                    tickmode='auto',
                    showticklabels=True
                )
            )
            st.plotly_chart(fig_amount, use_container_width=True)
        
        # Add metrics for the current status
        st.subheader("Current Payment Status")
        
        # Create metrics
        cols = st.columns(len(current_status))
        for i, (_, data) in enumerate(current_status.iterrows()):
            with cols[i]:
                st.metric(
                    f"{data['payment_status']}",
                    f"{data['count']:,} loans",
                    f"{data['percentage']:.1f}% of portfolio"
                )

    elif page == "Cohort Analysis":
        st.title("Cohort Analysis")
        
        # Load cohort data
        @st.cache_data(ttl="1h")
        def load_cohort_data():
            credentials = get_credentials()
            query = """
            WITH base_data AS (
                SELECT 
                    asset_id,
                    DATE_TRUNC(DATE(first_issue_date), MONTH) as cohort_month,
                    DATE(last_due_date) as analysis_date,
                    total_expected_amount,
                    total_paid_amount,
                    payment_status,
                    CASE 
                        WHEN payment_status IN ('FULLY_PAID_ON_TIME', 'FULLY_PAID_WITH_DELAYS') THEN 1
                        ELSE 0
                    END as is_paid
                FROM `credix-analytics.gold.fact_payment_performance`
                WHERE first_issue_date IS NOT NULL
                  AND last_due_date IS NOT NULL
            ),
            daily_stats AS (
                SELECT 
                    cohort_month,
                    analysis_date,
                    DATE_DIFF(analysis_date, cohort_month, DAY) as days_since_origination,
                    COUNT(DISTINCT asset_id) as total_loans,
                    SUM(total_expected_amount) as total_expected_amount,
                    SUM(total_paid_amount) as total_paid_amount,
                    COUNTIF(is_paid = 1) as paid_loans
                FROM base_data
                GROUP BY cohort_month, analysis_date, days_since_origination
            ),
            default_rates AS (
                SELECT 
                    cohort_month,
                    days_since_origination,
                    analysis_date,
                    total_loans,
                    total_paid_amount,
                    total_expected_amount,
                    paid_loans,
                    ROUND(100 * (1 - SAFE_DIVIDE(total_paid_amount, total_expected_amount)), 2) as daily_default_rate
                FROM daily_stats
            ),
            cumulative_stats AS (
                SELECT 
                    cohort_month,
                    days_since_origination,
                    total_loans,
                    MAX(daily_default_rate) OVER (
                        PARTITION BY cohort_month 
                        ORDER BY analysis_date 
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) as default_rate,
                    ROUND(100 * SAFE_DIVIDE(paid_loans, total_loans), 2) as paid_rate
                FROM default_rates
            )
            SELECT 
                cohort_month,
                days_since_origination,
                total_loans,
                default_rate,
                paid_rate
            FROM cumulative_stats
            WHERE cohort_month >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
            ORDER BY cohort_month, days_since_origination
            """
            return pd.read_gbq(query, credentials=credentials, project_id=credentials.project_id)

        df_cohort = load_cohort_data()
        
        # Format dates for better display
        df_cohort['cohort_month'] = pd.to_datetime(df_cohort['cohort_month'])
        df_cohort['cohort_month_str'] = df_cohort['cohort_month'].dt.strftime('%Y-%m')
        
        # Add cohort selection
        st.subheader("Cohort Selection")
        selected_cohorts = st.multiselect(
            "Select cohorts to analyze",
            options=sorted(df_cohort['cohort_month_str'].unique()),
            default=sorted(df_cohort['cohort_month_str'].unique())[-3:]  # Default to last 3 cohorts
        )
        
        if not selected_cohorts:
            st.warning("Please select at least one cohort to analyze")
            st.stop()
            
        # Filter data for selected cohorts
        df_selected = df_cohort[df_cohort['cohort_month_str'].isin(selected_cohorts)]
        
        # Create tabs for different analyses
        tab1, tab2 = st.tabs(["Remaining Balance Rate Evolution", "Fully Paid Loans Evolution"])
        
        with tab1:
            # Create line plot for default rate evolution
            fig_default = go.Figure()
            
            for cohort in selected_cohorts:
                cohort_data = df_selected[df_selected['cohort_month_str'] == cohort]
                
                fig_default.add_trace(
                    go.Scatter(
                        x=cohort_data['days_since_origination'],
                        y=cohort_data['default_rate'],
                        name=f"Cohort {cohort}",
                        mode='lines+markers'
                    )
                )
            
            fig_default.update_layout(
                title="Remaining Balance Rate Evolution by Cohort",
                xaxis_title="Days Since Origination",
                yaxis_title="Remaining Balance Rate (%)",
                plot_bgcolor='white',
                paper_bgcolor='white',
                yaxis=dict(
                    range=[0, 100],
                    gridcolor='rgba(0,0,0,0.1)',
                    tickformat='.1f'
                ),
                xaxis=dict(
                    gridcolor='rgba(0,0,0,0.1)',
                    tickmode='linear',
                    dtick=30  # Show tick every 30 days
                ),
                hovermode='x unified'
            )
            st.plotly_chart(fig_default, use_container_width=True)
            
        with tab2:
            # Create line plot for payment rate evolution
            fig_payment = go.Figure()
            
            for cohort in selected_cohorts:
                cohort_data = df_selected[df_selected['cohort_month_str'] == cohort]
                
                fig_payment.add_trace(
                    go.Scatter(
                        x=cohort_data['days_since_origination'],
                        y=cohort_data['paid_rate'],
                        name=f"Cohort {cohort}",
                        mode='lines+markers'
                    )
                )
            
            fig_payment.update_layout(
                title="Fully Paid Loans Evolution by Cohort",
                xaxis_title="Days Since Origination",
                yaxis_title="Fully Paid Loans (%)",
                plot_bgcolor='white',
                paper_bgcolor='white',
                yaxis=dict(
                    range=[0, 100],
                    gridcolor='rgba(0,0,0,0.1)',
                    tickformat='.1f'
                ),
                xaxis=dict(
                    gridcolor='rgba(0,0,0,0.1)',
                    tickmode='linear',
                    dtick=30  # Show tick every 30 days
                ),
                hovermode='x unified'
            )
            st.plotly_chart(fig_payment, use_container_width=True)
        
        # Show detailed metrics for selected cohorts
        st.subheader("Cohort Details")
        
        for cohort in selected_cohorts:
            st.write(f"**Cohort: {cohort}**")
            cohort_data = df_selected[df_selected['cohort_month_str'] == cohort].iloc[-1]  # Get latest data point
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric(
                    "Total Loans",
                    f"{int(cohort_data['total_loans']):,}"
                )
            
            with col2:
                st.metric(
                    "Remaining Balance Rate",
                    f"{cohort_data['default_rate']:.1f}%"
                )
            
            with col3:
                st.metric(
                    "Fully Paid Loans",
                    f"{cohort_data['paid_rate']:.1f}%"
                )
        
        # Add explanation of the visualizations
        st.markdown("""
        **Understanding the Analysis:**
        
        1. **Remaining Balance Rate Evolution:**
           - Each line represents a cohort of loans originated in a specific month
           - The x-axis shows days since the cohort's origination
           - The y-axis shows the percentage of expected amount not yet paid
           - The rate should increase over time as some loans become overdue
           - Higher lines indicate worse performance (more unpaid amounts)
           
        2. **Fully Paid Loans Evolution:**
           - Shows the percentage of loans in each cohort that have been fully paid
           - Higher percentages indicate better payment performance
           - The trend should generally increase over time as more loans are paid off
           
        3. **Cohort Details:**
           - Provides the latest metrics for each cohort
           - Helps compare the current state of different origination vintages
        """)

except Exception as e:
    st.error(f"An error occurred: {str(e)}")
    st.error("Please make sure you have set up the correct credentials in your secrets.toml file.") 