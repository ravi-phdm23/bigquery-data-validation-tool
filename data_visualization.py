#!/usr/bin/env python3
"""
Data Visualization Module
Handles dashboard creation, metrics display, and data visualization.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime
import json


def show_scenario_dashboard():
    """Display dashboard with scenario execution results."""
    if 'scenario_results' not in st.session_state or not st.session_state['scenario_results']:
        st.info("No scenario results available. Execute some scenarios first to see the dashboard.")
        return
    
    results = st.session_state['scenario_results']
    
    # Dashboard Header
    st.markdown("### Validation Dashboard")
    
    # Summary Metrics
    total_scenarios = len(results)
    passed_scenarios = len([r for r in results if r['status'] == 'PASS'])
    failed_scenarios = len([r for r in results if r['status'] in ['FAIL', 'ERROR']])
    success_rate = (passed_scenarios / total_scenarios * 100) if total_scenarios > 0 else 0
    
    # Display metrics in columns
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Total Scenarios",
            value=total_scenarios,
            delta=None
        )
    
    with col2:
        st.metric(
            label="Passed",
            value=passed_scenarios,
            delta=f"{success_rate:.1f}%"
        )
    
    with col3:
        st.metric(
            label="Failed/Error",
            value=failed_scenarios,
            delta=f"-{(failed_scenarios/total_scenarios*100):.1f}%" if total_scenarios > 0 else "0%"
        )
    
    with col4:
        total_rows_validated = sum([r.get('total_rows', 0) for r in results])
        st.metric(
            label="Rows Validated",
            value=f"{total_rows_validated:,}",
            delta="Total"
        )
    
    # Visualization Options
    viz_tab1, viz_tab2, viz_tab3, viz_tab4 = st.tabs(["Overview", "Detailed Results", "Scenario Analysis", "Export Results"])
    
    with viz_tab1:
        show_overview_charts(results)
    
    with viz_tab2:
        show_detailed_results_table(results)
    
    with viz_tab3:
        show_scenario_analysis(results)
    
    with viz_tab4:
        show_export_options()


def show_overview_charts(results):
    """Display overview charts for scenario results."""
    if not results:
        st.info("No data to display.")
        return
    
    # Prepare data for visualization
    df = pd.DataFrame(results)
    
    # Create charts in columns
    col1, col2 = st.columns(2)
    
    with col1:
        # Status Distribution Pie Chart
        status_counts = df['status'].value_counts()
        
        colors = {'PASS': '#28a745', 'FAIL': '#dc3545', 'ERROR': '#fd7e14'}
        fig_pie = px.pie(
            values=status_counts.values,
            names=status_counts.index,
            title="Validation Status Distribution",
            color=status_counts.index,
            color_discrete_map=colors
        )
        fig_pie.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig_pie, use_container_width=True)
    
    with col2:
        # Row Count Analysis
        if 'total_rows' in df.columns:
            fig_bar = px.bar(
                df,
                x='scenario_name',
                y='total_rows',
                color='status',
                title="Rows Validated by Scenario",
                color_discrete_map={'PASS': '#28a745', 'FAIL': '#dc3545', 'ERROR': '#fd7e14'}
            )
            fig_bar.update_xaxes(tickangle=45)
            fig_bar.update_layout(xaxis_title="Scenario", yaxis_title="Rows Validated")
            st.plotly_chart(fig_bar, use_container_width=True)
    
    # Timeline Chart (if timestamps available)
    if 'timestamp' in df.columns:
        st.markdown("#### ⏱️ Execution Timeline")
        df['timestamp_str'] = df['timestamp'].dt.strftime('%H:%M:%S')
        
        fig_timeline = px.scatter(
            df,
            x='timestamp',
            y='scenario_name',
            color='status',
            size='total_rows',
            hover_data=['total_rows', 'pass_rows', 'fail_rows'],
            title="Scenario Execution Timeline",
            color_discrete_map={'PASS': '#28a745', 'FAIL': '#dc3545', 'ERROR': '#fd7e14'}
        )
        fig_timeline.update_layout(xaxis_title="Execution Time", yaxis_title="Scenario")
        st.plotly_chart(fig_timeline, use_container_width=True)


def show_detailed_results_table(results):
    """Display detailed results in a table format."""
    if not results:
        st.info("No detailed results to display.")
        return
    
    st.markdown("#### Detailed Results Table")
    
    # Create detailed dataframe
    detailed_data = []
    for result in results:
        detailed_data.append({
            'Scenario': result.get('scenario_name', result.get('name', 'Unknown')),
            'Status': result['status'],
            'Total Rows': result.get('total_rows', 0),
            'Pass Rows': result.get('pass_rows', 0),
            'Fail Rows': result.get('fail_rows', 0),
            'Success Rate (%)': f"{(result.get('pass_rows', 0) / max(result.get('total_rows', 1), 1) * 100):.1f}",
            'Source Table': result.get('source_table', ''),
            'Target Table': result.get('target_table', ''),
            'Target Column': result.get('target_column', ''),
            'Execution Time': result['timestamp'].strftime('%Y-%m-%d %H:%M:%S') if 'timestamp' in result else ''
        })
    
    df_detailed = pd.DataFrame(detailed_data)
    
    # Display with filtering options
    col1, col2 = st.columns([1, 3])
    
    with col1:
        # Filter options
        status_filter = st.multiselect(
            "Filter by Status:",
            options=df_detailed['Status'].unique(),
            default=df_detailed['Status'].unique()
        )
        
        table_filter = st.selectbox(
            "Filter by Source Table:",
            options=["All"] + list(df_detailed['Source Table'].unique()),
            index=0
        )
    
    # Apply filters
    filtered_df = df_detailed[df_detailed['Status'].isin(status_filter)]
    if table_filter != "All":
        filtered_df = filtered_df[filtered_df['Source Table'] == table_filter]
    
    with col2:
        st.markdown(f"**Showing {len(filtered_df)} of {len(df_detailed)} scenarios**")
    
    # Display table with styling
    def color_status(val):
        if val == 'PASS':
            return 'background-color: #d4edda; color: #155724'
        elif val == 'FAIL':
            return 'background-color: #f8d7da; color: #721c24'
        elif val == 'ERROR':
            return 'background-color: #fff3cd; color: #856404'
        return ''
    
    styled_df = filtered_df.style.applymap(color_status, subset=['Status'])
    st.dataframe(styled_df, use_container_width=True, height=400)


def show_scenario_analysis(results):
    """Display detailed scenario analysis."""
    if not results:
        st.info("No data available for analysis.")
        return
    
    st.markdown("#### Scenario Analysis")
    
    # Select scenario for detailed view
    scenario_names = [r.get('scenario_name', r.get('name', 'Unknown')) for r in results]
    selected_scenario = st.selectbox("Select scenario for detailed analysis:", scenario_names)
    
    # Find selected scenario
    scenario_data = next((r for r in results if r.get('scenario_name', r.get('name', 'Unknown')) == selected_scenario), None)
    
    if scenario_data:
        col1, col2 = st.columns([2, 1])
        
        with col1:
            # Scenario Details
            st.markdown("##### 📊 Scenario Details")
            
            details_data = {
                'Property': [
                    'Status', 'Total Rows', 'Pass Rows', 'Fail Rows', 'Success Rate',
                    'Source Table', 'Target Table', 'Target Column', 'Execution Time'
                ],
                'Value': [
                    scenario_data['status'],
                    f"{scenario_data.get('total_rows', 0):,}",
                    f"{scenario_data.get('pass_rows', 0):,}",
                    f"{scenario_data.get('fail_rows', 0):,}",
                    f"{(scenario_data.get('pass_rows', 0) / max(scenario_data.get('total_rows', 1), 1) * 100):.1f}%",
                    scenario_data.get('source_table', 'N/A'),
                    scenario_data.get('target_table', 'N/A'),
                    scenario_data.get('target_column', 'N/A'),
                    scenario_data['timestamp'].strftime('%Y-%m-%d %H:%M:%S') if 'timestamp' in scenario_data else 'N/A'
                ]
            }
            
            details_df = pd.DataFrame(details_data)
            st.dataframe(details_df, use_container_width=True, hide_index=True)
        
        with col2:
            # Pass/Fail visualization for selected scenario
            if scenario_data.get('total_rows', 0) > 0:
                pass_count = scenario_data.get('pass_rows', 0)
                fail_count = scenario_data.get('fail_rows', 0)
                
                fig_donut = go.Figure(data=[go.Pie(
                    labels=['Pass', 'Fail'],
                    values=[pass_count, fail_count],
                    hole=.3,
                    marker_colors=['#28a745', '#dc3545']
                )])
                fig_donut.update_layout(title="Pass/Fail Distribution", height=300)
                st.plotly_chart(fig_donut, use_container_width=True)
        
        # Derivation Logic
        if 'derivation_logic' in scenario_data:
            st.markdown("##### 🔧 Derivation Logic")
            st.code(scenario_data['derivation_logic'], language='sql')
        
        # SQL Query
        if 'sql_logic' in scenario_data:
            with st.expander("📝 Generated SQL Query"):
                st.code(scenario_data['sql_logic'], language='sql')
        
        # Error details (if any)
        if scenario_data['status'] == 'ERROR' and 'error' in scenario_data:
            st.markdown("##### ❌ Error Details")
            st.error(scenario_data['error'])


def show_export_options():
    """Display export options for results."""
    st.markdown("#### 📤 Export Results")
    
    if 'scenario_results' not in st.session_state or not st.session_state['scenario_results']:
        st.info("No results to export. Execute scenarios first.")
        return
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("##### 📊 Summary Report")
        if st.button("📥 Download Summary CSV", type="primary"):
            download_summary_csv()
    
    with col2:
        st.markdown("##### 📋 Detailed Report")
        if st.button("📥 Download Detailed CSV"):
            download_detailed_csv()
    
    # Additional export options
    st.markdown("##### 🔧 Advanced Export Options")
    
    export_format = st.selectbox(
        "Select export format:",
        options=["CSV", "Excel (XLSX)", "JSON"],
        index=0
    )
    
    include_sql = st.checkbox("Include SQL queries in export", value=True)
    include_timestamps = st.checkbox("Include execution timestamps", value=True)
    
    if st.button("📤 Generate Custom Export"):
        generate_custom_export(export_format, include_sql, include_timestamps)


def download_summary_csv():
    """Generate and download summary CSV."""
    results = st.session_state['scenario_results']
    
    summary_data = []
    for result in results:
        summary_data.append({
            'Scenario_Name': result.get('scenario_name', result.get('name', 'Unknown')),
            'Status': result['status'],
            'Total_Rows': result.get('total_rows', 0),
            'Pass_Rows': result.get('pass_rows', 0),
            'Fail_Rows': result.get('fail_rows', 0),
            'Success_Rate_Percent': f"{(result.get('pass_rows', 0) / max(result.get('total_rows', 1), 1) * 100):.1f}",
            'Source_Table': result.get('source_table', ''),
            'Target_Table': result.get('target_table', ''),
            'Target_Column': result.get('target_column', ''),
            'Execution_Time': result['timestamp'].strftime('%Y-%m-%d %H:%M:%S') if 'timestamp' in result else ''
        })
    
    summary_df = pd.DataFrame(summary_data)
    csv_data = summary_df.to_csv(index=False)
    
    st.download_button(
        label="💾 Download Summary Report",
        data=csv_data,
        file_name=f"validation_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv"
    )


def download_detailed_csv():
    """Generate and download detailed CSV."""
    if 'detailed_results' not in st.session_state:
        st.error("No detailed results available.")
        return
    
    detailed_df = pd.DataFrame(st.session_state['detailed_results'])
    csv_data = detailed_df.to_csv(index=False)
    
    st.download_button(
        label="💾 Download Detailed Report",
        data=csv_data,
        file_name=f"validation_detailed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv"
    )


def generate_custom_export(export_format, include_sql, include_timestamps):
    """Generate custom export based on user preferences."""
    results = st.session_state['scenario_results']
    
    # Prepare data based on preferences
    export_data = []
    for result in results:
        row = {
            'Scenario_Name': result.get('scenario_name', result.get('name', 'Unknown')),
            'Status': result['status'],
            'Total_Rows': result.get('total_rows', 0),
            'Pass_Rows': result.get('pass_rows', 0),
            'Fail_Rows': result.get('fail_rows', 0),
            'Success_Rate_Percent': f"{(result.get('pass_rows', 0) / max(result.get('total_rows', 1), 1) * 100):.1f}",
            'Source_Table': result.get('source_table', ''),
            'Target_Table': result.get('target_table', ''),
            'Target_Column': result.get('target_column', ''),
            'Derivation_Logic': result.get('derivation_logic', '')
        }
        
        if include_sql and 'sql_logic' in result:
            row['SQL_Query'] = result['sql_logic']
        
        if include_timestamps and 'timestamp' in result:
            row['Execution_Time'] = result['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
        
        if result['status'] == 'ERROR' and 'error' in result:
            row['Error_Message'] = result['error']
        
        export_data.append(row)
    
    # Generate export based on format
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    if export_format == "CSV":
        export_df = pd.DataFrame(export_data)
        csv_data = export_df.to_csv(index=False)
        st.download_button(
            label="💾 Download Custom CSV",
            data=csv_data,
            file_name=f"validation_custom_{timestamp}.csv",
            mime="text/csv"
        )
    
    elif export_format == "JSON":
        json_data = json.dumps(export_data, indent=2, default=str)
        st.download_button(
            label="💾 Download Custom JSON",
            data=json_data,
            file_name=f"validation_custom_{timestamp}.json",
            mime="application/json"
        )
    
    elif export_format == "Excel (XLSX)":
        # For Excel export, we'll need to use BytesIO
        from io import BytesIO
        import xlsxwriter
        
        output = BytesIO()
        workbook = xlsxwriter.Workbook(output, {'in_memory': True})
        worksheet = workbook.add_worksheet('Validation_Results')
        
        # Write headers
        headers = list(export_data[0].keys()) if export_data else []
        for col, header in enumerate(headers):
            worksheet.write(0, col, header)
        
        # Write data
        for row, data in enumerate(export_data, 1):
            for col, header in enumerate(headers):
                worksheet.write(row, col, data.get(header, ''))
        
        workbook.close()
        output.seek(0)
        
        st.download_button(
            label="💾 Download Custom Excel",
            data=output,
            file_name=f"validation_custom_{timestamp}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )


def show_real_time_monitoring():
    """Display real-time monitoring dashboard (if implemented)."""
    st.markdown("#### 📡 Real-time Monitoring")
    st.info("🚧 Real-time monitoring features will be implemented in future versions.")
    
    # Placeholder for future real-time features
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Active Connections", "2", "↑1")
    
    with col2:
        st.metric("Queries/Min", "15", "↑3")
    
    with col3:
        st.metric("Avg Response Time", "1.2s", "↓0.3s")


def create_comparison_chart(data1, data2, title="Comparison Chart"):
    """Create comparison charts between different datasets."""
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=('Dataset 1', 'Dataset 2'),
        specs=[[{"type": "bar"}, {"type": "bar"}]]
    )
    
    # Add data1 bars
    fig.add_trace(
        go.Bar(x=data1.index, y=data1.values, name="Dataset 1"),
        row=1, col=1
    )
    
    # Add data2 bars
    fig.add_trace(
        go.Bar(x=data2.index, y=data2.values, name="Dataset 2"),
        row=1, col=2
    )
    
    fig.update_layout(title_text=title, showlegend=False)
    return fig
