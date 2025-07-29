#!/usr/bin/env python3
"""
Streamlit Frontend for BigQuery Test Scenarios - Fixed Version
A user-friendly web interface to run BigQuery testing scenarios with interactive results.
"""

import streamlit as st
import pandas as pd
from datetime import datetime

# Import modular components
from bigquery_client import connect_to_bigquery, initialize_session_state, execute_custom_query
from sql_generator import (
    create_transformation_validation_sql,
    create_enhanced_transformation_sql,
    create_reference_table_validation_sql
)
from excel_handler import (
    generate_scenarios_from_excel,
    process_excel_file,
    execute_all_excel_scenarios,
    generate_sql_for_scenario,
    get_scenario_type
)
from data_visualization import (
    show_scenario_dashboard,
    show_overview_charts,
    show_detailed_results_table,
    show_scenario_analysis,
    show_export_options
)

# Page configuration
st.set_page_config(
    page_title="BigQuery Data Validation Tool",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .scenario-header {
        font-size: 1.5rem;
        color: #ff7f0e;
        margin: 1rem 0;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    .success-message {
        color: #28a745;
        font-weight: bold;
    }
    .error-message {
        color: #dc3545;
        font-weight: bold;
    }
    .info-box {
        background-color: #e3f2fd;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #2196f3;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)


def main():
    """Main application function."""
    # Initialize session state
    initialize_session_state()
    
    # App header
    st.markdown('<h1 class="main-header">BigQuery Data Validation Tool</h1>', unsafe_allow_html=True)
    
    # Sidebar for configuration
    with st.sidebar:
        st.markdown("### Configuration")
        
        # BigQuery connection settings
        st.markdown("#### BigQuery Connection")
        project_id = st.text_input(
            "🏢 Project ID:",
            value=st.session_state.get('project_id', ''),
            help="Your Google Cloud BigQuery project ID"
        )
        
        dataset_id = st.text_input(
            "📊 Dataset ID:",
            value=st.session_state.get('dataset_id', ''),
            help="Your BigQuery dataset name"
        )
        
        # Auto-save to session state when values change
        if project_id != st.session_state.get('project_id', ''):
            st.session_state['project_id'] = project_id
        if dataset_id != st.session_state.get('dataset_id', ''):
            st.session_state['dataset_id'] = dataset_id
        
        # Connection button
        if st.button("🔌 Connect to BigQuery", type="primary"):
            if project_id and dataset_id:
                # Save to session state
                st.session_state['project_id'] = project_id
                st.session_state['dataset_id'] = dataset_id
                
                success, message = connect_to_bigquery(project_id, dataset_id)
                if success:
                    st.success(message)
                else:
                    st.error(message)
            else:
                st.error("❌ Please provide both Project ID and Dataset ID")
        
        # Connection status
        if st.session_state.get('connection_status') == 'connected':
            st.success("✅ Connected to BigQuery")
            st.info(f"📊 Project: `{st.session_state.get('project_id')}`\n📁 Dataset: `{st.session_state.get('dataset_id')}`")
        elif st.session_state.get('connection_status') == 'failed':
            st.error("❌ Connection failed")
        else:
            if st.session_state.get('project_id'):
                st.info("📋 Project ID configured - Ready to connect")
            else:
                st.warning("⚠️ Project ID required for BigQuery access")
        
        # Quick actions
        st.markdown("#### Quick Actions")
        
        if st.button("Reset Session"):
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.rerun()
        
        if st.button("Clear Results"):
            if 'scenario_results' in st.session_state:
                del st.session_state['scenario_results']
            if 'detailed_results' in st.session_state:
                del st.session_state['detailed_results']
            st.success("Results cleared!")
    
    # Main content area
    if st.session_state.get('connection_status') != 'connected':
        show_welcome_screen()
    else:
        show_main_interface()


def show_welcome_screen():
    """Display welcome screen when not connected."""
    st.markdown("""
    <div class="info-box">
        <h3>👋 Welcome to BigQuery Data Validation Tool</h3>
        <p>This tool helps you validate data transformations between BigQuery tables using Excel-based mapping files.</p>
    </div>
    """, unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Key Features")
        st.markdown("""
        - **Excel-Based Mapping**: Define validation scenarios in Excel
        - **Reference Table Support**: Handle lookup tables and VLOOKUPs
        - **Business Logic Parsing**: Convert business rules to SQL
        - **Interactive Dashboard**: Visualize validation results
        - **Export Reports**: Download detailed validation reports
        - **Automated SQL Generation**: No need to write complex SQL
        """)
    
    with col2:
        st.markdown("### Getting Started")
        st.markdown("""
        1. **Connect to BigQuery**: Enter your Project ID and Dataset ID in the sidebar
        2. **Upload Excel File**: Use the Excel Mapping tab to upload your validation scenarios
        3. **Execute Scenarios**: Run all validations with one click
        4. **View Results**: Analyze results in the Data Visualization dashboard
        5. **Export Reports**: Download comprehensive validation reports
        """)
    
    # Sample data section
    st.markdown("### Sample Excel Format")
    show_sample_excel_preview()


def show_sample_excel_preview():
    """Show a preview of the expected Excel format."""
    sample_data = pd.DataFrame({
        'Source_Table': ['customers', 'accounts', 'transactions'],
        'Target_Table': ['customer_summary', 'account_summary', 'transaction_summary'],
        'Source_Join_Key': ['customer_id', 'account_id', 'transaction_id'],
        'Target_Join_Key': ['cust_id', 'acct_id', 'trans_id'],
        'Target_Column': ['full_name', 'balance', 'amount_sum'],
        'Derivation_Logic': [
            'CONCAT(first_name, " ", last_name)',
            'current_balance',
            'SUM(amount) GROUP_BY account_id'
        ],
        'Validation_Type': ['Transformation', 'Direct_Copy', 'Aggregation']
    })
    
    st.markdown("**Sample Excel Mapping Structure:**")
    st.dataframe(sample_data, use_container_width=True)


def show_main_interface():
    """Display main interface when connected to BigQuery."""
    # Create tabs for different functionalities
    tab1, tab2, tab3 = st.tabs([
        "Excel Mapping", 
        "Data Visualization", 
        "Documentation"
    ])
    
    with tab1:
        show_excel_mapping_tab()
    
    with tab2:
        show_data_visualization_tab()
    
    with tab3:
        show_documentation_tab()


def show_excel_mapping_tab():
    """Display Excel mapping interface."""
    st.markdown("### Excel-Based Validation Mapping")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # File upload
        uploaded_file = st.file_uploader(
            "📤 Upload Excel Mapping File",
            type=['xlsx', 'xls'],
            help="Upload your Excel file containing validation scenarios"
        )
        
        if uploaded_file is not None:
            # Process the uploaded file
            excel_data, error = process_excel_file(uploaded_file)
            
            if error:
                st.error(error)
            else:
                st.success(f"✅ Successfully loaded Excel file with {len(excel_data)} sheet(s)")
                
                # Sheet selection
                sheet_names = list(excel_data.keys())
                selected_sheet = st.selectbox(
                    "📋 Select sheet to process:",
                    sheet_names,
                    help="Choose the sheet containing your validation mapping"
                )
                
                # Display sheet preview
                if selected_sheet:
                    df = excel_data[selected_sheet]
                    st.markdown(f"**Preview of '{selected_sheet}' sheet ({len(df)} rows):**")
                    st.dataframe(df.head(10), use_container_width=True)
                    
                    # Generate scenarios button
                    if st.button("Generate Validation Scenarios", type="primary"):
                        with st.spinner("Generating scenarios..."):
                            scenarios = generate_scenarios_from_excel(
                                df, 
                                st.session_state.get('project_id'),
                                st.session_state.get('dataset_id')
                            )
                            
                            if scenarios:
                                st.session_state['excel_scenarios'] = scenarios
                                st.success(f"✅ Generated {len(scenarios)} validation scenarios!")
                            else:
                                st.warning("⚠️ No valid scenarios could be generated from the Excel file.")
    
    # Show scenarios preview and SQL section if scenarios exist - this is outside the button conditional
    if 'excel_scenarios' in st.session_state and st.session_state['excel_scenarios']:
        scenarios = st.session_state['excel_scenarios']
        
        st.markdown("---")
        st.markdown("#### Generated Scenarios Preview")
        scenario_preview = pd.DataFrame([
            {
                'Scenario': s['scenario_name'],
                'Source Table': s['source_table'],
                'Target Table': s['target_table'],
                'Target Column': s['target_column'],
                'Logic': s['derivation_logic'][:50] + '...' if len(s['derivation_logic']) > 50 else s['derivation_logic'],
                'Type': get_scenario_type(s)
            }
            for s in scenarios
        ])
        st.dataframe(scenario_preview, use_container_width=True)
        
        # SQL Preview Section - Collapsed by default
        with st.expander("🔍 SQL Preview", expanded=False):
            st.markdown("Select a scenario below to view its generated SQL:")
            
            # Create scenario selection dropdown with stable options
            scenario_options = [f"{s['scenario_name']} - {get_scenario_type(s)}" for s in scenarios]
            
            # Use session state to maintain selection
            if 'selected_sql_scenario' not in st.session_state:
                st.session_state.selected_sql_scenario = scenario_options[0] if scenario_options else None
            
            selected_scenario_name = st.selectbox(
                "Choose scenario to view SQL:",
                scenario_options,
                index=scenario_options.index(st.session_state.selected_sql_scenario) if st.session_state.selected_sql_scenario in scenario_options else 0,
                key="sql_scenario_dropdown"
            )
            
            # Update session state
            st.session_state.selected_sql_scenario = selected_scenario_name
            
            # Find the selected scenario
            selected_scenario = None
            for scenario in scenarios:
                if f"{scenario['scenario_name']} - {get_scenario_type(scenario)}" == selected_scenario_name:
                    selected_scenario = scenario
                    break
            
            if selected_scenario:
                # Show scenario details in columns
                st.markdown(f"#### Scenario Details: {selected_scenario['scenario_name']}")
                
                col1, col2, col3 = st.columns([1, 1, 1])
                with col1:
                    st.markdown(f"**Source Table:** `{selected_scenario['source_table']}`")
                    st.markdown(f"**Source Dataset ID:** `{selected_scenario.get('source_dataset_id', 'banking_sample_data')}`")
                    st.markdown(f"**Target Column:** `{selected_scenario['target_column']}`")
                
                with col2:
                    st.markdown(f"**Target Table:** `{selected_scenario.get('target_table', 'N/A')}`")
                    st.markdown(f"**Target Dataset ID:** `{selected_scenario.get('target_dataset_id', 'banking_sample_data')}`")
                    st.markdown(f"**Validation Type:** {get_scenario_type(selected_scenario)}")
                
                with col3:
                    st.markdown(f"**Join Keys:** `{selected_scenario.get('source_join_key', 'id')}` → `{selected_scenario.get('target_join_key', 'id')}`")
                    if selected_scenario.get('reference_table') and str(selected_scenario.get('reference_table')).lower() not in ['nan', 'none', '']:
                        st.markdown(f"**Reference Table:** `{selected_scenario['reference_table']}`")
                    else:
                        st.markdown("**Reference Table:** N/A")
                
                # Derivation Logic
                st.markdown(f"**Derivation Logic:** `{selected_scenario['derivation_logic']}`")
                
                # Check if this is a cross-dataset scenario and highlight it
                source_dataset = selected_scenario.get('source_dataset_id', 'banking_sample_data')
                target_dataset = selected_scenario.get('target_dataset_id', 'banking_sample_data')
                
                if source_dataset != target_dataset:
                    st.info(f"🔄 **Cross-Dataset Validation:** This scenario validates data across different datasets ({source_dataset} → {target_dataset})")
                elif source_dataset != 'banking_sample_data':
                    st.info(f"📊 **Custom Dataset:** This scenario uses dataset: {source_dataset}")
                
                # SQL section (not nested in expander since we're already inside the SQL Preview expander)
                st.markdown("#### Generated SQL Query")
                # Cache SQL generation in session state to avoid regeneration
                sql_cache_key = f"sql_cache_{selected_scenario['scenario_name']}"
                
                if sql_cache_key not in st.session_state:
                    with st.spinner("Generating SQL..."):
                        try:
                            sql_query = generate_sql_for_scenario(
                                selected_scenario,
                                st.session_state.get('project_id', 'cohesive-apogee-411113'),
                                st.session_state.get('dataset_id', 'banking_sample_data')
                            )
                            st.session_state[sql_cache_key] = sql_query
                        except Exception as e:
                            st.session_state[sql_cache_key] = f"-- Error generating SQL: {str(e)}"
                
                sql_query = st.session_state[sql_cache_key]
                
                if sql_query and not sql_query.startswith("-- Error"):
                    st.code(sql_query, language='sql')
                    
                    # Add download button for SQL
                    st.download_button(
                        label="📥 Download SQL",
                        data=sql_query,
                        file_name=f"{selected_scenario['scenario_name']}_validation.sql",
                        mime="text/sql",
                        key=f"download_sql_{selected_scenario['scenario_name']}"
                    )
                    
                    st.info("💡 You can copy the SQL above and run it directly in BigQuery console.")
                else:
                    st.error(f"Failed to generate SQL: {sql_query}")
            else:
                st.warning("No scenario selected or scenario not found.")
        
        # Execute scenarios section
        st.markdown("---")
        st.markdown("### Execute Validation Scenarios")
        
        col1, col2, col3 = st.columns([1, 1, 1])
        
        with col1:
            st.metric("Generated Scenarios", len(scenarios))
        
        with col2:
            if st.button("Execute All Scenarios", type="primary"):
                execute_all_excel_scenarios()
        
        with col3:
            if 'scenario_results' in st.session_state:
                passed = len([r for r in st.session_state['scenario_results'] if r['status'] == 'PASS'])
                st.metric("Scenarios Passed", f"{passed}/{len(st.session_state['scenario_results'])}")


def show_data_visualization_tab():
    """Display data visualization dashboard."""
    st.markdown("### Data Visualization Dashboard")
    
    # Show dashboard if results are available
    show_scenario_dashboard()


def show_documentation_tab():
    """Display comprehensive documentation."""
    st.markdown("### Documentation")
    
    doc_tab1, doc_tab2, doc_tab3 = st.tabs(["🎯 Quick Start", "📊 Excel Format", "🔧 Advanced Features"])
    
    with doc_tab1:
        st.markdown("""
        #### Quick Start Guide
        
        1. **Setup Connection**
           - Enter your Google Cloud Project ID
           - Provide BigQuery Dataset ID
           - Click "Connect to BigQuery"
        
        2. **Prepare Excel File**
           - Create Excel file with validation scenarios
           - Include required columns: Source_Table, Target_Column, Derivation_Logic
           - See Excel Format tab for detailed requirements
        
        3. **Upload and Execute**
           - Upload Excel file in the Excel Mapping tab
           - Generate scenarios from your mapping
           - Execute all scenarios with one click
        
        4. **Analyze Results**
           - View results in Data Visualization tab
           - Export detailed reports
           - Monitor validation metrics
        """)
    
    with doc_tab2:
        st.markdown("""
        #### Excel Format Requirements
        
        The Excel file format guide has been removed for simplicity. 
        Please use the example scenarios provided or contact support for format details.
        """)
    
    with doc_tab3:
        st.markdown("""
        #### Advanced Features
        
        **Reference Table Support:**
        - Handle VLOOKUP operations
        - Support multiple table joins
        - Business logic with lookup tables
        
        **Business Logic Parsing:**
        - Convert plain English to SQL
        - Support IF-THEN-ELSE conditions
        - Handle complex aggregations
        
        **Export Options:**
        - CSV, Excel, JSON formats
        - Include SQL queries in exports
        - Detailed execution reports
        """)


if __name__ == "__main__":
    main()
