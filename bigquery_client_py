#!/usr/bin/env python3
"""
BigQuery Client Module
Handles BigQuery connections and query execution.
"""

import streamlit as st
import pandas as pd
from google.cloud import bigquery
import logging
from datetime import datetime


def connect_to_bigquery(project_id, dataset_id):
    """Initialize BigQuery connection."""
    try:
        # Configure logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        logger = logging.getLogger(__name__)
        
        logger.info(f"Initializing BigQuery client for project: {project_id}")
        
        # Initialize BigQuery client
        client = bigquery.Client(project=project_id)
        
        # Store client and dataset in session state
        st.session_state.bigquery_client = client
        st.session_state.project_id = project_id
        st.session_state.dataset_id = dataset_id
        st.session_state.connection_status = "connected"
        
        logger.info("✅ BigQuery client initialized successfully")
        return True, "✅ Successfully connected to BigQuery!"
        
    except Exception as e:
        st.session_state.connection_status = "failed"
        logging.error(f"❌ BigQuery connection failed: {str(e)}")
        return False, f"❌ Connection error: {str(e)}"


def execute_custom_query(query, query_name):
    """Execute a custom BigQuery query."""
    if st.session_state.connection_status != "connected":
        return None, "❌ Not connected to BigQuery"
    
    try:
        client = st.session_state.bigquery_client
        job = client.query(query)
        results = job.result()
        
        # Convert to pandas DataFrame
        df = results.to_dataframe()
        return {
            'status': 'success',
            'data': df,
            'row_count': len(df),
            'timestamp': datetime.now()
        }, f"✅ Query executed successfully - {len(df)} rows returned"
        
    except Exception as e:
        return {
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now()
        }, f"❌ Query execution failed: {str(e)}"


def initialize_session_state():
    """Initialize session state variables."""
    if 'test_scenarios' not in st.session_state:
        st.session_state.test_scenarios = None
    if 'results_cache' not in st.session_state:
        st.session_state.results_cache = {}
    if 'connection_status' not in st.session_state:
        st.session_state.connection_status = None
