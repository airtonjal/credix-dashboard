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
    SELECT *
    FROM `gold.fact_portfolio_risk`
    ORDER BY snapshot_date
    """
    return pd.read_gbq(query, credentials=credentials, project_id=credentials.project_id)

@st.cache_data(ttl="1h")
def load_payment_performance():
    credentials = get_credentials()
    query = """
    SELECT 
        pp.*,
        b.buyer_tax_id as company_id,
        b.main_cnae as industry_sector,
        b.uf as state_code,
        b.company_size,
        b.risk_category
    FROM `gold.fact_payment_performance` pp
    JOIN `gold.dim_borrower` b ON pp.borrower_key = b.borrower_key
    """
    return pd.read_gbq(query, credentials=credentials, project_id=credentials.project_id)

# Sidebar
st.sidebar.title("Credix Analytics")
page = st.sidebar.selectbox(
    "Choose a Dashboard",
    ["Portfolio Overview", "Risk Analysis", "Payment Behavior"]
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
            total_loans = len(df_loan['loan_id'].unique())
            st.metric("Total Loans", f"{total_loans:,}")
            
        with col2:
            total_borrowers = len(df_loan['borrower_key'].unique())
            st.metric("Total Borrowers", f"{total_borrowers:,}")
            
        with col3:
            total_amount = df_loan['loan_amount'].sum()
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
            title="Distribution of Loan Payment Status"
        )
        st.plotly_chart(fig_status, use_container_width=True)

        # Industry Distribution
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Loans by Industry")
            industry_dist = df_loan.groupby('industry_sector')['loan_amount'].sum().sort_values(ascending=True)
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
            state_dist = df_loan.groupby('state_code')['loan_amount'].sum().sort_values(ascending=False)
            fig_geo = px.bar(
                x=state_dist.index,
                y=state_dist.values,
                title="Total Loan Amount by State (UF)"
            )
            fig_geo.update_layout(xaxis_title="State (UF)", yaxis_title="Total Loan Amount (R$)")
            st.plotly_chart(fig_geo, use_container_width=True)

        # Add Company Size Distribution
        st.subheader("Company Size Distribution")
        size_dist = df_loan.groupby('company_size')['loan_amount'].sum().sort_values(ascending=True)
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
        risk_dist = df_loan.groupby('risk_category')['loan_amount'].sum().sort_values(ascending=True)
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
        
        # Risk Metrics Over Time
        st.subheader("Risk Metrics Trend")
        fig_risk = go.Figure()
        
        metrics = ['default_rate', 'recovery_rate', 'loss_given_default']
        for metric in metrics:
            fig_risk.add_trace(
                go.Scatter(
                    x=df_risk['snapshot_date'],
                    y=df_risk[metric],
                    name=metric.replace('_', ' ').title()
                )
            )
        
        fig_risk.update_layout(
            title="Risk Metrics Over Time",
            xaxis_title="Date",
            yaxis_title="Rate",
            hovermode='x unified'
        )
        st.plotly_chart(fig_risk, use_container_width=True)
        
        # Cohort Analysis
        st.subheader("Cohort Analysis")
        cohort_matrix = df_risk.pivot(
            index='snapshot_date',
            columns='cohort_month',
            values='default_rate'
        )
        
        fig_cohort = px.imshow(
            cohort_matrix,
            title="Default Rate by Cohort",
            labels=dict(x="Cohort Month", y="Snapshot Date", color="Default Rate"),
            aspect="auto"
        )
        st.plotly_chart(fig_cohort, use_container_width=True)

    else:  # Payment Behavior
        st.title("Payment Behavior Analysis")
        
        # Load data
        df_payment = load_payment_performance()
        
        # Payment Status Timeline
        st.subheader("Payment Status Over Time")
        status_time = df_payment.groupby(['snapshot_date', 'payment_status']).size().unstack(fill_value=0)
        
        fig_timeline = px.area(
            status_time,
            title="Payment Status Distribution Over Time"
        )
        fig_timeline.update_layout(
            xaxis_title="Date",
            yaxis_title="Number of Loans",
            hovermode='x unified'
        )
        st.plotly_chart(fig_timeline, use_container_width=True)
        
        # Payment Behavior by Industry
        st.subheader("Payment Behavior by Industry")
        industry_payment = df_payment.groupby(['industry_sector', 'payment_status']).size().unstack(fill_value=0)
        
        fig_industry = px.bar(
            industry_payment,
            barmode='stack',
            title="Payment Status Distribution by Industry"
        )
        fig_industry.update_layout(
            xaxis_title="Industry",
            yaxis_title="Number of Loans"
        )
        st.plotly_chart(fig_industry, use_container_width=True)

except Exception as e:
    st.error(f"An error occurred: {str(e)}")
    st.error("Please make sure you have set up the correct credentials in your secrets.toml file.") 