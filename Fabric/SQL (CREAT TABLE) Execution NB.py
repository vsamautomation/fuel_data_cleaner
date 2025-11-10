#!/usr/bin/env python
# coding: utf-8

# ## SQL (CREAT TABLE) Execution NB
# 
# New notebook

# In[1]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %pip install -q -U semantic-link-labs google-genai anthropic typing_extensions pydantic


# In[6]:


# SQL Execution Notebook for Fabric Warehouse
# This notebook reads SQL CREATE TABLE files from the lakehouse and executes them in a Fabric warehouse

# Imports
import os
import re
from typing import List, Optional, Dict
import sempy_labs
from sempy_labs import ConnectWarehouse
import sempy.fabric as fabric

# Configuration
WAREHOUSE_ID = ""  # Update this with your warehouse ID
SQL_SCRIPTS_BASE_PATH = "/lakehouse/default/Files/sql_scripts"

print("✅ SQL Execution Notebook initialized")
print(f"📦 Warehouse ID: {WAREHOUSE_ID}")
print(f"📁 SQL Scripts Path: {SQL_SCRIPTS_BASE_PATH}")


# In[4]:


fabric.list_workspaces()


# In[7]:


sempy_labs.list_warehouses()


# In[10]:


def read_sql_file(dataset_id: str) -> str:
    """
    Read SQL file from lakehouse and clean markdown code block markers.
    
    Args:
        dataset_id: The dataset ID to locate the SQL file
    
    Returns:
        Cleaned SQL content as string
    
    Raises:
        FileNotFoundError: If SQL file doesn't exist
    """
    sql_filepath = f"{SQL_SCRIPTS_BASE_PATH}/{dataset_id}/{dataset_id}_create_tables.sql"
    
    if not os.path.exists(sql_filepath):
        raise FileNotFoundError(f"SQL file not found: {sql_filepath}")
    
    with open(sql_filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Remove markdown code block markers
    content = re.sub(r'^```sql\s*', '', content, flags=re.MULTILINE)
    content = re.sub(r'^```\s*$', '', content, flags=re.MULTILINE)
    
    return content.strip()

def split_sql_statements(sql_content: str) -> List[str]:
    """
    Split SQL content into individual CREATE TABLE statements.
    Handles multi-line statements with nested brackets and comments.
    
    Args:
        sql_content: The SQL content to split
    
    Returns:
        List of individual CREATE TABLE statements
    """
    statements = []
    
    # Split by lines and process line by line to handle multi-line statements
    lines = sql_content.split('\n')
    current_statement = []
    in_create_table = False
    
    for line in lines:
        stripped = line.strip()
        
        # Skip empty lines and pure comment lines when not in a statement
        if not in_create_table and (not stripped or stripped.startswith('--')):
            continue
        
        # Check if this line starts a CREATE TABLE
        if re.match(r'CREATE\s+TABLE', stripped, re.IGNORECASE):
            # If we were building a statement, save it first
            if current_statement:
                stmt = '\n'.join(current_statement).strip()
                if stmt:
                    statements.append(stmt)
            current_statement = [line]
            in_create_table = True
        elif in_create_table:
            current_statement.append(line)
            # Check if this line ends the statement (semicolon at end)
            if stripped.endswith(';'):
                stmt = '\n'.join(current_statement).strip()
                if stmt:
                    statements.append(stmt)
                current_statement = []
                in_create_table = False
    
    # Handle any remaining statement
    if current_statement:
        stmt = '\n'.join(current_statement).strip()
        if stmt:
            statements.append(stmt)
    
    return statements


def execute_sql_in_warehouse(warehouse_id: str, sql_statements: List[str], dataset_id: str) -> Dict:
    """
    Execute SQL statements in a Fabric warehouse one at a time.
    Stops on first error for debugging.
    
    Args:
        warehouse_id: The warehouse ID to connect to
        sql_statements: List of SQL statements to execute
        dataset_id: Dataset ID for logging purposes
    
    Returns:
        Dictionary with execution results
    
    Raises:
        Exception: On first SQL execution error
    """
    results = {
        'dataset_id': dataset_id,
        'total_statements': len(sql_statements),
        'executed': 0,
        'failed': 0,
        'errors': []
    }
    
    print(f"\n{'='*80}")
    print(f"🚀 Executing SQL for dataset: {dataset_id}")
    print(f"📊 Total statements: {len(sql_statements)}")
    print(f"{'='*80}\n")
    
    try:
        with ConnectWarehouse(warehouse_id) as conn:
            for idx, statement in enumerate(sql_statements, 1):
                # Extract table name for logging (if possible)
                table_match = re.search(r'CREATE\s+TABLE\s+(?:\[)?([^\s\]]+)(?:\])?', statement, re.IGNORECASE)
                table_name = table_match.group(1) if table_match else f"Statement {idx}"
                
                print(f"  [{idx}/{len(sql_statements)}] Executing: {table_name}")
                
                try:
                    # Execute the statement
                    conn.query(statement)
                    results['executed'] += 1
                    print(f"    ✅ Success: {table_name}")
                    
                except Exception as e:
                    results['failed'] += 1
                    error_info = {
                        'statement_number': idx,
                        'table_name': table_name,
                        'error': str(e),
                        'sql_snippet': statement[:200] + "..." if len(statement) > 200 else statement
                    }
                    results['errors'].append(error_info)
                    
                    print(f"\n    ❌ ERROR on statement {idx} ({table_name})")
                    print(f"    Error: {str(e)}")
                    print(f"    SQL snippet: {statement[:200]}...")
                    print(f"\n{'='*80}")
                    print(f"🛑 STOPPING: First error encountered (debugging mode)")
                    print(f"{'='*80}\n")
                    
                    # Stop on first error
                    raise Exception(f"Failed to execute statement {idx} ({table_name}): {str(e)}") from e
    
    except Exception as e:
        # Re-raise to stop execution
        raise
    
    print(f"\n✅ Completed: {results['executed']}/{results['total_statements']} statements executed successfully")
    
    return results


# In[11]:


def execute_sql_files(dataset_ids: Optional[List[str]] = None) -> Dict:
    """
    Execute SQL files from the lakehouse in a Fabric warehouse.
    
    Args:
        dataset_ids: Optional list of specific dataset IDs to process.
                     If None, processes all SQL files found in the directory.
    
    Returns:
        Dictionary with execution results for all processed datasets
    """
    all_results = {
        'processed_datasets': [],
        'total_datasets': 0,
        'successful': 0,
        'failed': 0,
        'results': []
    }
    
    # Discover SQL files
    if dataset_ids is None:
        print(f"🔍 Discovering SQL files in: {SQL_SCRIPTS_BASE_PATH}")
        dataset_ids = []
        
        if os.path.exists(SQL_SCRIPTS_BASE_PATH):
            for item in os.listdir(SQL_SCRIPTS_BASE_PATH):
                dataset_dir = os.path.join(SQL_SCRIPTS_BASE_PATH, item)
                if os.path.isdir(dataset_dir):
                    sql_file = os.path.join(dataset_dir, f"{item}_create_tables.sql")
                    if os.path.exists(sql_file):
                        dataset_ids.append(item)
        
        print(f"  ✅ Found {len(dataset_ids)} SQL files")
        if not dataset_ids:
            print(f"  ⚠️  No SQL files found in {SQL_SCRIPTS_BASE_PATH}")
            return all_results
    else:
        print(f"📋 Processing {len(dataset_ids)} specified dataset(s)")
    
    all_results['total_datasets'] = len(dataset_ids)
    
    # Process each dataset
    for idx, dataset_id in enumerate(dataset_ids, 1):
        print(f"\n{'='*80}")
        print(f"📦 [{idx}/{len(dataset_ids)}] Processing dataset: {dataset_id}")
        print(f"{'='*80}")
        
        try:
            # Read SQL file
            print(f"📖 Reading SQL file...")
            sql_content = read_sql_file(dataset_id)
            print(f"  ✅ File read successfully ({len(sql_content)} characters)")
            
            # Split into statements
            print(f"✂️  Splitting SQL statements...")
            statements = split_sql_statements(sql_content)
            print(f"  ✅ Found {len(statements)} CREATE TABLE statement(s)")
            
            if not statements:
                print(f"  ⚠️  No CREATE TABLE statements found in file")
                continue
            
            # Execute in warehouse
            result = execute_sql_in_warehouse(WAREHOUSE_ID, statements, dataset_id)
            
            all_results['processed_datasets'].append(dataset_id)
            all_results['results'].append(result)
            all_results['successful'] += 1
            
            print(f"\n✅ Dataset {dataset_id} completed successfully")
            
        except FileNotFoundError as e:
            print(f"\n❌ File not found: {e}")
            all_results['failed'] += 1
            all_results['results'].append({
                'dataset_id': dataset_id,
                'status': 'failed',
                'error': str(e)
            })
            
        except Exception as e:
            print(f"\n❌ Error processing dataset {dataset_id}: {e}")
            all_results['failed'] += 1
            all_results['results'].append({
                'dataset_id': dataset_id,
                'status': 'failed',
                'error': str(e)
            })
            
            # In debugging mode, we stop on first error
            print(f"\n{'='*80}")
            print(f"🛑 STOPPING: Error encountered (debugging mode)")
            print(f"{'='*80}\n")
            break
    
    # Summary
    print(f"\n{'='*80}")
    print(f"📊 EXECUTION SUMMARY")
    print(f"{'='*80}")
    print(f"  ✅ Total Datasets: {all_results['total_datasets']}")
    print(f"  ✅ Successful: {all_results['successful']}")
    print(f"  ❌ Failed: {all_results['failed']}")
    print(f"{'='*80}\n")
    
    return all_results


# # Usage Examples
# 
# ## Example 1: Execute All SQL Files
# Process all SQL files found in the lakehouse directory.

# In[ ]:


# Example 1: Execute all SQL files
results = execute_sql_files()


# ## Example 2: Execute Specific Dataset IDs
# Process only specific datasets by providing a list of dataset IDs.

# In[12]:


specific_datasets = [
    "4b08eee3-3c01-441c-b7bd-0ec50e7aa12f"
]

results = execute_sql_files(dataset_ids=specific_datasets)

