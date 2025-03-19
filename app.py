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
    FROM `gold.fact_loan_performance` lp
    JOIN `gold.dim_borrower` b ON lp.borrower_key = b.borrower_key
    """
    return pd.read_gbq(query, credentials=credentials, project_id=credentials.project_id)

@st.cache_data(ttl="1h")
def load_portfolio_risk():
    credentials = get_credentials()
    query = """
    WITH dates AS (
        SELECT DISTINCT cohort_month as date
        FROM `gold.fact_portfolio_risk`
        ORDER BY date
    ),
    monthly_stats AS (
        SELECT 
            cohort_month as date,
            default_rate,
            npl_ratio,
            avg_max_days_late,
            avg_installments_over_30d_late,
            fully_paid_on_time_amount,
            fully_paid_delayed_amount,
            overdue_amount,
            npl_amount,
            total_portfolio_value
        FROM `gold.fact_portfolio_risk`
        WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM `gold.fact_portfolio_risk`)  -- Get latest snapshot
    )
    SELECT 
        d.date,
        ms.*
    FROM dates d
    LEFT JOIN monthly_stats ms ON d.date = ms.date
    ORDER BY d.date
    """
    return pd.read_gbq(query, credentials=credentials, project_id=credentials.project_id)

@st.cache_data(ttl="1h")
def load_payment_performance():
    credentials = get_credentials()
    query = """
    WITH latest_snapshot AS (
        SELECT MAX(snapshot_date) as latest_date
        FROM `gold.fact_payment_performance`
    ),
    current_status AS (
        SELECT 
            payment_status,
            COUNT(*) as count,
            SUM(total_original_amount) as total_amount
        FROM `gold.fact_payment_performance`
        WHERE snapshot_date = (SELECT latest_date FROM latest_snapshot)
        GROUP BY payment_status
    ),
    historical_stats AS (
        SELECT 
            DATE(last_due_date) as date,
            payment_status,
            COUNT(*) as count,
            SUM(total_original_amount) as total_amount
        FROM `gold.fact_payment_performance`
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
    ["Portfolio Overview", "Risk Analysis", "Payment Behavior"],
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
        st.title("Risk Analysis")
        
        # Load data
        df_risk = load_portfolio_risk()
        
        # Convert date to datetime and format it
        df_risk['date'] = pd.to_datetime(df_risk['date']).dt.strftime('%Y-%m-%d')
        
        # Risk Metrics Over Time
        st.subheader("Risk Metrics Trend")
        
        # Create two columns for different metrics
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Default and NPL Ratios")
            fig_default = go.Figure()
            
            metrics = ['default_rate', 'npl_ratio']
            for metric in metrics:
                fig_default.add_trace(
                    go.Scatter(
                        x=df_risk['date'],
                        y=df_risk[metric],
                        name=metric.replace('_', ' ').title(),
                        hovertemplate="%{y:.2%}<extra></extra>"
                    )
                )
            
            fig_default.update_layout(
                title="Default Rate and NPL Ratio Over Time",
                xaxis_title="Date",
                yaxis_title="Rate",
                hovermode='x unified',
                yaxis_tickformat='.2%',  # Format y-axis as percentage
                xaxis=dict(
                    type='category',  # Force categorical x-axis
                    tickangle=-45,  # Angle the dates for better readability
                    dtick=1  # Show all dates
                )
            )
            st.plotly_chart(fig_default, use_container_width=True)
        
        with col2:
            st.subheader("Late Payment Metrics")
            fig_late = go.Figure()
            
            metrics = ['avg_max_days_late', 'avg_installments_over_30d_late']
            for metric in metrics:
                fig_late.add_trace(
                    go.Scatter(
                        x=df_risk['date'],
                        y=df_risk[metric],
                        name=metric.replace('_', ' ').title()
                    )
                )
            
            fig_late.update_layout(
                title="Late Payment Metrics Over Time",
                xaxis_title="Date",
                yaxis_title="Value",
                hovermode='x unified',
                xaxis=dict(
                    type='category',  # Force categorical x-axis
                    tickangle=-45,  # Angle the dates for better readability
                    dtick=1  # Show all dates
                )
            )
            st.plotly_chart(fig_late, use_container_width=True)
        
        # Portfolio Composition
        st.subheader("Portfolio Composition")
        
        # Calculate percentages for each category
        df_risk['on_time_pct'] = df_risk['fully_paid_on_time_amount'] / df_risk['total_portfolio_value'] * 100
        df_risk['delayed_pct'] = df_risk['fully_paid_delayed_amount'] / df_risk['total_portfolio_value'] * 100
        df_risk['overdue_pct'] = df_risk['overdue_amount'] / df_risk['total_portfolio_value'] * 100
        df_risk['npl_pct'] = df_risk['npl_amount'] / df_risk['total_portfolio_value'] * 100
        
        fig_composition = go.Figure()
        
        categories = [
            ('on_time_pct', 'Paid On Time'),
            ('delayed_pct', 'Paid with Delay'),
            ('overdue_pct', 'Overdue'),
            ('npl_pct', 'NPL')
        ]
        
        for col, name in categories:
            fig_composition.add_trace(
                go.Scatter(
                    x=df_risk['date'],
                    y=df_risk[col],
                    name=name,
                    stackgroup='one',
                    hovertemplate="%{y:.1f}%<extra></extra>"
                )
            )
        
        fig_composition.update_layout(
            title="Portfolio Composition Over Time",
            xaxis_title="Date",
            yaxis_title="Percentage of Portfolio",
            hovermode='x unified',
            yaxis_range=[0, 100],
            yaxis_ticksuffix='%',
            xaxis=dict(
                type='category',  # Force categorical x-axis
                tickangle=-45,  # Angle the dates for better readability
                dtick=1  # Show all dates
            )
        )
        st.plotly_chart(fig_composition, use_container_width=True)
        
        # Cohort Analysis
        st.subheader("Cohort Analysis")
        
        # Convert cohort_month to datetime for better display
        df_risk['cohort_month'] = pd.to_datetime(df_risk['cohort_month'])
        
        cohort_matrix = df_risk.pivot(
            index='date',
            columns='cohort_month',
            values='default_rate'
        )
        
        fig_cohort = px.imshow(
            cohort_matrix,
            title="Default Rate by Cohort",
            labels=dict(
                x="Cohort Month",
                y="Snapshot Date",
                color="Default Rate"
            ),
            aspect="auto",
            color_continuous_scale="RdYlBu_r"
        )
        
        # Format the values as percentages
        fig_cohort.update_traces(
            hovertemplate="Cohort Month: %{x}<br>Snapshot Date: %{y}<br>Default Rate: %{z:.2%}<extra></extra>"
        )
        
        st.plotly_chart(fig_cohort, use_container_width=True)

    else:  # Payment Behavior
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

except Exception as e:
    st.error(f"An error occurred: {str(e)}")
    st.error("Please make sure you have set up the correct credentials in your secrets.toml file.") 