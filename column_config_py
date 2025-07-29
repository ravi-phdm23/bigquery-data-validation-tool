#!/usr/bin/env python3
"""
Column Configuration Module
Handles configurable column mappings for different database schemas.
"""

import os
import importlib.util
import logging

# Default column mappings for banking tables (fallback)
DEFAULT_BANKING_MAPPINGS = {
    'customers': {
        'columns': ['customer_id', 'first_name', 'last_name', 'full_name', 'account_number', 
                   'account_type', 'balance', 'account_open_date', 'address', 'city', 
                   'state', 'zip_code', 'risk_score', 'account_status', 'monthly_income'],
        'name_fields': {
            'first_name': 'first_name',
            'last_name': 'last_name',
            'full_name': 'full_name'
        },
        'mappings': {
            'email': 'address',  # Map email requests to address field
            'customer_id': 'customer_id',
            'first_name': 'first_name',
            'last_name': 'last_name',
            'full_name': 'full_name'
        }
    },
    'transactions': {
        'columns': ['transaction_id', 'account_number', 'transaction_type', 'amount', 
                   'transaction_date', 'channel', 'merchant', 'transaction_city', 
                   'transaction_state', 'status', 'is_fraudulent', 'processing_fee'],
        'name_fields': {},
        'mappings': {
            'transaction_id': 'transaction_id',
            'amount': 'amount',
            'transaction_date': 'transaction_date'
        }
    },
    'account_profiles': {
        'columns': ['customer_reference', 'account_id', 'current_balance', 'account_status', 
                   'account_type', 'last_transaction_date', 'credit_limit'],
        'name_fields': {},
        'mappings': {
            'account_id': 'account_id',
            'current_balance': 'current_balance'
        }
    }
}

def load_office_database_config():
    """Load office database configuration if available."""
    try:
        from office_database_config import OFFICE_DATABASE_MAPPINGS
        print("✅ Loaded office database configuration")
        return OFFICE_DATABASE_MAPPINGS
    except ImportError:
        print("ℹ️  Using default banking configuration (office_database_config.py not found)")
        return DEFAULT_BANKING_MAPPINGS

def load_external_config(config_file_path):
    """Load configuration from external Python file."""
    try:
        spec = importlib.util.spec_from_file_location("external_config", config_file_path)
        config_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(config_module)
        
        if hasattr(config_module, 'DATABASE_MAPPINGS'):
            print(f"✅ Loaded external configuration from {config_file_path}")
            return config_module.DATABASE_MAPPINGS
        else:
            print(f"⚠️  Configuration file {config_file_path} doesn't contain DATABASE_MAPPINGS")
            return DEFAULT_BANKING_MAPPINGS
    except Exception as e:
        print(f"❌ Failed to load external configuration: {e}")
        return DEFAULT_BANKING_MAPPINGS

# Try to load the most appropriate configuration
DEFAULT_COLUMN_MAPPINGS = load_office_database_config()

class ColumnConfigManager:
    """Manages column configurations for different table schemas."""
    
    def __init__(self, custom_mappings=None, config_file=None):
        """Initialize with default, custom, or file-based column mappings."""
        if config_file:
            self.mappings = load_external_config(config_file)
        elif custom_mappings:
            self.mappings = custom_mappings
        else:
            self.mappings = DEFAULT_COLUMN_MAPPINGS.copy()
        
        print(f"📋 Column configuration loaded with {len(self.mappings)} table(s): {list(self.mappings.keys())}")
    
    def load_from_file(self, config_file_path):
        """Load configuration from external file."""
        self.mappings = load_external_config(config_file_path)
    
    def switch_to_banking_schema(self):
        """Switch to banking schema (original default)."""
        self.mappings = DEFAULT_BANKING_MAPPINGS.copy()
        print("🏦 Switched to banking database schema")
    
    def switch_to_office_schema(self):
        """Switch to office schema."""
        self.mappings = load_office_database_config()
        print("🏢 Switched to office database schema")
    
    def list_available_tables(self):
        """List all configured tables."""
        return list(self.mappings.keys())
    
    def describe_table(self, table_name):
        """Get detailed information about a table configuration."""
        table_name = table_name.lower()
        if table_name not in self.mappings:
            return f"Table '{table_name}' not found in configuration"
        
        config = self.mappings[table_name]
        description = f"""
Table: {table_name}
Columns ({len(config['columns'])}): {', '.join(config['columns'])}
Name Fields: {config['name_fields']}
Column Mappings: {config['mappings']}
        """
        return description.strip()
    
    def get_table_columns(self, table_name):
        """Get column list for a specific table."""
        table_name = table_name.lower()
        if table_name in self.mappings:
            return self.mappings[table_name]['columns']
        return ['*']  # Default fallback
    
    def get_column_mapping(self, table_name):
        """Get column mapping dictionary for a specific table."""
        table_name = table_name.lower()
        if table_name in self.mappings:
            return self.mappings[table_name]['mappings']
        return {}
    
    def get_name_fields(self, table_name):
        """Get name field mappings for a specific table."""
        table_name = table_name.lower()
        if table_name in self.mappings:
            return self.mappings[table_name]['name_fields']
        return {}
    
    def map_column(self, table_name, requested_column):
        """Map a requested column to actual table column."""
        mappings = self.get_column_mapping(table_name)
        return mappings.get(requested_column, requested_column)
    
    def has_column(self, table_name, column_name):
        """Check if a table has a specific column."""
        columns = self.get_table_columns(table_name)
        return column_name.lower() in [col.lower() for col in columns]
    
    def get_name_concat_expression(self, table_name):
        """Get the appropriate name concatenation expression for a table."""
        name_fields = self.get_name_fields(table_name)
        columns = self.get_table_columns(table_name)
        
        # Check if we have separate first_name and last_name fields
        first_name_field = name_fields.get('first_name')
        last_name_field = name_fields.get('last_name')
        
        if (first_name_field and last_name_field and 
            self.has_column(table_name, first_name_field) and 
            self.has_column(table_name, last_name_field)):
            return f'CONCAT({first_name_field}, " ", {last_name_field})'
        
        # Check if we have a full_name field
        full_name_field = name_fields.get('full_name')
        if full_name_field and self.has_column(table_name, full_name_field):
            return full_name_field
        
        # Fallback - try to find any name-like field
        for col in columns:
            if 'name' in col.lower():
                return col
        
        # Final fallback
        return '"Unknown Name"'
    
    def add_custom_table(self, table_name, columns, name_fields=None, mappings=None):
        """Add a custom table configuration."""
        self.mappings[table_name.lower()] = {
            'columns': columns,
            'name_fields': name_fields or {},
            'mappings': mappings or {}
        }
    
    def update_table_config(self, table_name, **kwargs):
        """Update configuration for an existing table."""
        table_name = table_name.lower()
        if table_name not in self.mappings:
            self.mappings[table_name] = {'columns': [], 'name_fields': {}, 'mappings': {}}
        
        for key, value in kwargs.items():
            if key in ['columns', 'name_fields', 'mappings']:
                self.mappings[table_name][key] = value
        
        print(f"✅ Updated configuration for table '{table_name}'")
    
    def configure_office_table(self, table_name, columns, id_column=None, name_column=None, email_column=None):
        """Helper method to quickly configure an office table."""
        config = {
            'columns': columns,
            'name_fields': {},
            'mappings': {}
        }
        
        # Auto-configure common mappings
        if id_column:
            config['mappings']['id'] = id_column
            config['mappings'][f'{table_name}_id'] = id_column
        
        if name_column:
            config['name_fields']['full_name'] = name_column
            config['name_fields']['first_name'] = name_column
            config['name_fields']['last_name'] = name_column
            config['mappings']['name'] = name_column
        
        if email_column:
            config['mappings']['email'] = email_column
        
        self.mappings[table_name.lower()] = config
        print(f"🔧 Configured office table '{table_name}' with {len(columns)} columns")
        return config
    
    def auto_detect_columns(self, table_name, sample_columns):
        """Auto-detect and configure columns based on common patterns."""
        config = {
            'columns': sample_columns,
            'name_fields': {},
            'mappings': {}
        }
        
        # Common patterns for different field types
        id_patterns = ['id', '_id', 'key', 'code']
        name_patterns = ['name', 'title', 'description']
        email_patterns = ['email', 'mail', 'contact']
        date_patterns = ['date', 'time', 'created', 'updated']
        
        for col in sample_columns:
            col_lower = col.lower()
            
            # Detect ID columns
            if any(pattern in col_lower for pattern in id_patterns):
                config['mappings']['id'] = col
                config['mappings'][f'{table_name}_id'] = col
            
            # Detect name columns
            if any(pattern in col_lower for pattern in name_patterns):
                config['name_fields']['full_name'] = col
                config['mappings']['name'] = col
            
            # Detect email columns
            if any(pattern in col_lower for pattern in email_patterns):
                config['mappings']['email'] = col
            
            # Detect date columns
            if any(pattern in col_lower for pattern in date_patterns):
                config['mappings']['date'] = col
        
        self.mappings[table_name.lower()] = config
        print(f"🤖 Auto-configured table '{table_name}' with detected patterns")
        return config

# Global instance for backward compatibility
default_column_config = ColumnConfigManager()

def get_column_config():
    """Get the default column configuration manager."""
    return default_column_config

def set_custom_column_config(custom_mappings):
    """Set custom column mappings globally."""
    global default_column_config
    default_column_config = ColumnConfigManager(custom_mappings)
