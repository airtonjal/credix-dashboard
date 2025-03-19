# Credix Analytics Dashboard

A Streamlit dashboard for visualizing credit risk analytics data from BigQuery.

## Features

- **Portfolio Overview**: View key metrics, loan status distribution, and portfolio breakdowns by industry and geography.
- **Risk Analysis**: Track risk metrics over time and analyze cohort performance.
- **Payment Behavior**: Analyze payment patterns and industry-specific payment behavior.

## Setup

1. Create a Streamlit Community Cloud account at https://streamlit.io/cloud

2. Fork this repository to your GitHub account

3. Create a `.streamlit/secrets.toml` file with your BigQuery credentials:
   ```toml
   [gcp_service_account]
   type = "service_account"
   project_id = "your-project-id"
   private_key_id = "your-private-key-id"
   private_key = "your-private-key"
   client_email = "your-service-account-email"
   client_id = "your-client-id"
   auth_uri = "https://accounts.google.com/o/oauth2/auth"
   token_uri = "https://oauth2.googleapis.com/token"
   auth_provider_x509_cert_url = "https://www.googleapis.com/oauth2/v1/certs"
   client_x509_cert_url = "your-cert-url"
   ```

4. Deploy to Streamlit Community Cloud:
   - Connect your GitHub repository
   - Select the `streamlit/app.py` file
   - Deploy! 