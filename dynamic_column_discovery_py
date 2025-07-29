#!/usr/bin/env python3
"""
Dynamic Column Discovery Module
Automatically discovers table schemas from BigQuery and Excel scenarios.
"""

import logging
from typing import Dict, List, Any, Optional, Set
from bigquery_client import execute_custom_query

# Try to import standalone client for non-Streamlit environments
try:
    from standalone_bigquery import execute_standalone_query
    STANDALONE_AVAILABLE = True
except ImportError:
    STANDALONE_AVAILABLE = False

class DynamicColumnManager:
    """Dynamically discovers and manages column information from database and scenarios."""
    
    def __init__(self, project_id: str = None, dataset_id: str = None):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_schemas = {}  # Cache for discovered schemas
        self.column_patterns = {}  # Discovered column patterns
        self.logger = logging.getLogger(__name__)
    
    def discover_table_schema(self, project_id: str, dataset_id: str, table_name: str) -> Dict[str, Any]:
        """Dynamically discover table schema from BigQuery."""
        cache_key = f"{project_id}.{dataset_id}.{table_name}"
        if cache_key in self.table_schemas:
            return self.table_schemas[cache_key]
        
        try:
            # Query BigQuery information schema to get column details
            schema_query = f"""
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default,
                CASE 
                    WHEN LOWER(column_name) LIKE '%id%' OR LOWER(column_name) LIKE '%key%' THEN 'id'
                    WHEN LOWER(column_name) LIKE '%name%' OR LOWER(column_name) LIKE '%title%' THEN 'name'
                    WHEN LOWER(column_name) LIKE '%email%' OR LOWER(column_name) LIKE '%mail%' THEN 'email'
                    WHEN LOWER(column_name) LIKE '%date%' OR LOWER(column_name) LIKE '%time%' THEN 'date'
                    WHEN LOWER(column_name) LIKE '%amount%' OR LOWER(column_name) LIKE '%balance%' OR LOWER(column_name) LIKE '%salary%' THEN 'numeric'
                    WHEN LOWER(column_name) LIKE '%address%' OR LOWER(column_name) LIKE '%location%' THEN 'address'
                    WHEN LOWER(column_name) LIKE '%phone%' OR LOWER(column_name) LIKE '%mobile%' THEN 'phone'
                    ELSE 'other'
                END as column_category
            FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = '{table_name}'
            ORDER BY ordinal_position
            """
            
            result, message = execute_custom_query(schema_query, "schema_discovery")
            
            # Fallback to standalone client if Streamlit is not available
            if result is None and STANDALONE_AVAILABLE:
                try:
                    result = execute_standalone_query(schema_query, project_id)
                    message = "Success using standalone client"
                except Exception as e:
                    result = None
                    message = f"Standalone query failed: {e}"
            
            # Extract result data if it's in the new format
            result_data = result
            if isinstance(result, dict) and 'data' in result:
                result_data = result['data']
            
            if result_data is not None and not result_data.empty:
                columns = result_data['column_name'].tolist()
                column_types = dict(zip(result_data['column_name'], result_data['data_type']))
                column_categories = dict(zip(result_data['column_name'], result_data['column_category']))
                
                schema = {
                    'columns': columns,
                    'column_types': column_types,
                    'column_categories': column_categories,
                    'name_fields': self._detect_name_fields(columns, column_categories),
                    'mappings': self._create_smart_mappings(columns, column_categories)
                }
                
                self.table_schemas[cache_key] = schema
                self.logger.info(f"✅ Discovered schema for {table_name}: {len(columns)} columns")
                return schema
            else:
                self.logger.warning(f"⚠️  Could not discover schema for table {table_name}: {message}")
                return self._create_fallback_schema(table_name)
                
        except Exception as e:
            self.logger.error(f"❌ Error discovering schema for {table_name}: {e}")
            return self._create_fallback_schema(table_name)
    
    def _detect_name_fields(self, columns: List[str], categories: Dict[str, str]) -> Dict[str, str]:
        """Detect name-related fields in the table."""
        name_fields = {}
        
        # Look for specific name patterns
        for col in columns:
            col_lower = col.lower()
            if 'first_name' in col_lower or 'fname' in col_lower:
                name_fields['first_name'] = col
            elif 'last_name' in col_lower or 'lname' in col_lower:
                name_fields['last_name'] = col
            elif 'full_name' in col_lower or col_lower in ['name', 'employee_name', 'customer_name']:
                name_fields['full_name'] = col
        
        # If no specific name fields found, use the first name-category column
        if not name_fields:
            for col, category in categories.items():
                if category == 'name':
                    name_fields['full_name'] = col
                    break
        
        return name_fields
    
    def _create_smart_mappings(self, columns: List[str], categories: Dict[str, str]) -> Dict[str, str]:
        """Create smart column mappings based on discovered patterns."""
        mappings = {}
        
        # Create mappings for each category
        for col, category in categories.items():
            col_lower = col.lower()
            
            # ID mappings
            if category == 'id':
                mappings['id'] = col
                if 'customer' in col_lower:
                    mappings['customer_id'] = col
                elif 'employee' in col_lower:
                    mappings['employee_id'] = col
                elif 'project' in col_lower:
                    mappings['project_id'] = col
            
            # Name mappings
            elif category == 'name':
                mappings['name'] = col
                mappings['full_name'] = col
                if 'first' in col_lower:
                    mappings['first_name'] = col
                elif 'last' in col_lower:
                    mappings['last_name'] = col
            
            # Email mappings
            elif category == 'email':
                mappings['email'] = col
            
            # Date mappings
            elif category == 'date':
                mappings['date'] = col
                if 'transaction' in col_lower:
                    mappings['transaction_date'] = col
                elif 'hire' in col_lower:
                    mappings['hire_date'] = col
            
            # Numeric mappings
            elif category == 'numeric':
                if 'amount' in col_lower:
                    mappings['amount'] = col
                elif 'balance' in col_lower:
                    mappings['balance'] = col
                elif 'salary' in col_lower:
                    mappings['salary'] = col
        
        return mappings
    
    def _create_fallback_schema(self, table_name: str) -> Dict[str, Any]:
        """Create a fallback schema when discovery fails."""
        return {
            'columns': ['*'],  # Wildcard - will work but less optimal
            'column_types': {},
            'column_categories': {},
            'name_fields': {},
            'mappings': {}
        }
    
    def _find_table_key(self, table_name: str) -> str:
        """Find the cache key for a table name."""
        for key in self.table_schemas.keys():
            # Cache key format is "project_id.dataset_id.table_name"
            if key.endswith(f".{table_name}"):
                return key
        return None
    
    def get_table_columns(self, table_key: str) -> List[str]:
        """Get column list for a table using cache key."""
        if table_key in self.table_schemas:
            return self.table_schemas[table_key]['columns']
        return []
    
    def get_column_mapping(self, table_key: str) -> Dict[str, str]:
        """Get column mappings for a table using cache key."""
        if table_key in self.table_schemas:
            return self.table_schemas[table_key]['mappings']
        return {}
    
    def get_name_fields(self, table_key: str) -> Dict[str, str]:
        """Get name field mappings for a table using cache key."""
        if table_key in self.table_schemas:
            return self.table_schemas[table_key]['name_fields']
        return {}
    
    def map_column(self, table_name: str, requested_column: str) -> str:
        """Map a requested column to actual table column."""
        table_key = self._find_table_key(table_name)
        if not table_key:
            return requested_column
        mappings = self.get_column_mapping(table_key)
        return mappings.get(requested_column, requested_column)
    
    def has_column(self, table_key: str, column_name: str) -> bool:
        """Check if table has specific column."""
        if not table_key:
            return False
        columns = self.get_table_columns(table_key)
        if columns == ['*']:
            return True  # Assume column exists if using wildcard
        return column_name.lower() in [col.lower() for col in columns]
    
    def get_name_concat_expression(self, table_name: str) -> str:
        """Generate appropriate name concatenation expression."""
        # Find the correct cache key for this table
        table_key = self._find_table_key(table_name)
        if not table_key:
            # Try to discover schema if not found
            if self.project_id and self.dataset_id:
                self.discover_table_schema(self.project_id, self.dataset_id, table_name)
                table_key = self._find_table_key(table_name)
        
        name_fields = self.get_name_fields(table_key) if table_key else {}
        columns = self.get_table_columns(table_key) if table_key else []
        
        # Check for separate first/last name fields
        first_name = name_fields.get('first_name')
        last_name = name_fields.get('last_name')
        
        if first_name and last_name and self.has_column(table_key, first_name) and self.has_column(table_key, last_name):
            return f'CONCAT({first_name}, " ", {last_name})'
        
        # Check for full name field
        full_name = name_fields.get('full_name')
        if full_name and self.has_column(table_key, full_name):
            return full_name
        
        # Try to find any name-like column
        for col in columns:
            if 'name' in col.lower():
                return col
        
        # Final fallback - try common name column patterns
        common_name_patterns = ['full_name', 'name', 'employee_name', 'customer_name', 'first_name', 'fname']
        for pattern in common_name_patterns:
            if self.has_column(table_key, pattern):
                return pattern
        
        # Ultimate fallback
        return '"Unknown Name"'
    
    def analyze_derivation_logic(self, derivation_logic: str, table_name: str) -> Dict[str, Any]:
        """Analyze derivation logic to understand required columns."""
        analysis = {
            'referenced_columns': [],
            'operations': [],
            'complexity': 'simple'
        }
        
        # Extract column references from logic
        logic_lower = derivation_logic.lower()
        table_key = self._find_table_key(table_name)
        columns = self.get_table_columns(table_key) if table_key else []
        
        for col in columns:
            if col.lower() in logic_lower:
                analysis['referenced_columns'].append(col)
        
        # Detect operations
        if 'concat' in logic_lower:
            analysis['operations'].append('concatenation')
        if any(op in logic_lower for op in ['sum', 'count', 'avg', 'max', 'min']):
            analysis['operations'].append('aggregation')
        if 'case when' in logic_lower:
            analysis['operations'].append('conditional')
        
        # Determine complexity
        if len(analysis['operations']) > 1:
            analysis['complexity'] = 'complex'
        elif len(analysis['operations']) == 1:
            analysis['complexity'] = 'moderate'
        
        return analysis
    
    def discover_all_tables_from_scenarios(self, scenarios: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """Discover schemas for all tables mentioned in scenarios."""
        discovered_schemas = {}
        unique_tables = set()
        
        # Extract all table names from scenarios
        for scenario in scenarios:
            if 'source_table' in scenario:
                unique_tables.add(scenario['source_table'])
            if 'target_table' in scenario:
                unique_tables.add(scenario['target_table'])
        
        # Discover schema for each unique table
        for table_name in unique_tables:
            if table_name:  # Skip empty table names
                # Use project_id and dataset_id from instance, or get from scenarios if available
                project_id = getattr(self, 'project_id', None) or scenarios[0].get('source_dataset_id', 'banking_sample_data')
                dataset_id = getattr(self, 'dataset_id', None) or scenarios[0].get('source_dataset_id', 'banking_sample_data')
                schema = self.discover_table_schema(project_id, dataset_id, table_name)
                discovered_schemas[table_name] = schema
        
        return discovered_schemas
    
    def get_schema_summary(self) -> Dict[str, Any]:
        """Get summary of all discovered schemas."""
        summary = {
            'total_tables': len(self.table_schemas),
            'tables': {},
            'common_patterns': self._analyze_common_patterns()
        }
        
        for table_name, schema in self.table_schemas.items():
            summary['tables'][table_name] = {
                'column_count': len(schema['columns']),
                'has_name_fields': bool(schema['name_fields']),
                'has_id_fields': any('id' in cat for cat in schema['column_categories'].values()),
                'categories': list(set(schema['column_categories'].values()))
            }
        
        return summary
    
    def _analyze_common_patterns(self) -> Dict[str, Any]:
        """Analyze common patterns across all discovered schemas."""
        patterns = {
            'common_column_names': set(),
            'common_categories': set(),
            'naming_conventions': []
        }
        
        all_columns = []
        all_categories = []
        
        for schema in self.table_schemas.values():
            all_columns.extend(schema['columns'])
            all_categories.extend(schema['column_categories'].values())
        
        # Find common patterns
        from collections import Counter
        column_counts = Counter(all_columns)
        category_counts = Counter(all_categories)
        
        # Columns that appear in multiple tables
        patterns['common_column_names'] = {col for col, count in column_counts.items() if count > 1}
        patterns['common_categories'] = set(category_counts.keys())
        
        return patterns

# Global instance for easy access
_dynamic_column_manager = None

def get_dynamic_column_manager(project_id: str = None, dataset_id: str = None) -> DynamicColumnManager:
    """Get or create the dynamic column manager instance."""
    global _dynamic_column_manager
    
    if _dynamic_column_manager is None:
        _dynamic_column_manager = DynamicColumnManager(project_id, dataset_id)
    elif project_id and dataset_id and (_dynamic_column_manager.project_id != project_id or _dynamic_column_manager.dataset_id != dataset_id):
        # Create new instance if project/dataset changed
        _dynamic_column_manager = DynamicColumnManager(project_id, dataset_id)
    
    return _dynamic_column_manager

def reset_dynamic_column_manager():
    """Reset the global dynamic column manager (useful for testing)."""
    global _dynamic_column_manager
    _dynamic_column_manager = None
