#!/usr/bin/env python
# coding: utf-8

# ## AI Notebook 2
# 
# New notebook

# In[1]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %pip install -q -U semantic-link-labs google-genai anthropic typing_extensions pydantic


# In[2]:


# Imports
import sempy
import sempy_labs
import sempy.fabric as fabric
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col
from google import genai
from google.genai import types
import pandas as pd
import numpy as np
import anthropic
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import time
import json
import os
import re

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

print("✅ All imports successful and Spark session initialized")


# In[3]:


# Power BI to T-SQL data type mapping
DATATYPE_MAPPING = {
    # Integer types
    'Int64': 'BIGINT',
    'Int32': 'INT',
    'Integer': 'INT',
    'Whole Number': 'INT',

    # NUMERIC / Numeric types
    'NUMERIC': 'NUMERIC(18, 2)',
    'Fixed DECIMAL Number': 'NUMERIC(18, 2)',
    'Currency': 'NUMERIC(19, 4)',
    'Double': 'NUMERIC(18,2)',
    'Percentage': 'NUMERIC(18,2)',

    # String / Text types
    'String': 'VARCHAR(255)',
    'Text': 'VARCHAR(8000)',
    'Large Text': 'VARCHAR(MAX)',

    # Date / Time types
    'DateTime': 'DATETIME2(6)',
    'DateTimeZone': 'DATETIME2(6)',
    'Date': 'DATE',
    'Time': 'TIME(6)',

    # Boolean
    'Boolean': 'BIT',
    'True/False': 'BIT',

    # Binary / Other
    'Binary': 'VARBINARY(MAX)',
    'Guid': 'UNIQUEIDENTIFIER',
    'Variant': 'VARCHAR(255)',
    'Unknown': 'VARCHAR(255)'
}


# In[4]:


@dataclass
class ColumnSpec:
    """Column specification for T-SQL generation"""
    column_name: str
    data_type: str
    tsql_data_type: str
    is_nullable: bool = True
    is_primary_key: bool = False
    is_foreign_key: bool = False
    referenced_table: Optional[str] = None
    referenced_column: Optional[str] = None


@dataclass
class TableSpec:
    """Table specification for T-SQL generation"""
    table_name: str
    columns: List[ColumnSpec]
    relationships_from: List[Dict]
    relationships_to: List[Dict]
    usage_metrics: Dict


@dataclass
class DatasetMigrationSpec:
    """Complete dataset migration specification"""
    dataset_id: str
    dataset_name: str
    workspace_id: str
    workspace_name: str
    tables: List[TableSpec]
    excluded_tables: List[str]
    excluded_columns: int
    excluded_measures: int
    total_relationships: int


@dataclass
class MCodeExpression:
    """Represents a single M code expression from a dataset"""
    dataset_id: str
    dataset_name: str
    workspace_id: str
    workspace_name: str
    table_name: str
    expression: str
    expression_type: str
    object_name: Optional[str] = None
    column_name: Optional[str] = None
    
    def get_context(self) -> str:
        """Get a formatted context string for AI prompts"""
        return f"""
Dataset: {self.dataset_name}
Workspace: {self.workspace_name}
Table: {self.table_name}
Expression Type: {self.expression_type}
Object: {self.object_name or 'N/A'}
"""


@dataclass
class MCodeExtractionResult:
    """Results from M code extraction"""
    dataset_id: str
    dataset_name: str
    workspace_name: str
    expressions: List[MCodeExpression]
    total_expressions: int
    tables_with_expressions: List[str]
    extraction_timestamp: str


# In[7]:


class TSQLMigrationPrep:
    """Prepares Power BI datasets for T-SQL migration"""
    
    def __init__(self, lakehouse: Optional[str] = None, api_key: Optional[str] = None, agent_mode: Optional[str] = None):
        self.lakehouse = lakehouse
        self.api_key = api_key
        self.agent_mode = agent_mode
        self.client = None

        if api_key and agent_mode:
            if agent_mode == "claude":
                self.client = anthropic.Anthropic(api_key=api_key)
            elif agent_mode == "gemini":
                self.client = genai.Client(api_key=api_key)
            else:
                raise ValueError("Agent mode must be 'claude' or 'gemini'")

        self.column_usage_df = pd.DataFrame()
        self.table_analysis_df = pd.DataFrame()
        self.dataset_analysis_df = pd.DataFrame()
        self.relationships_df = pd.DataFrame()
        
        print("✅ T-SQL Migration Prep initialized")
    
    def _make_json_safe(self, obj):
        """Recursively converts any object into JSON-serializable types."""
        if isinstance(obj, dict):
            return {k: self._make_json_safe(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._make_json_safe(v) for v in obj]
        elif isinstance(obj, (datetime, pd.Timestamp)):
            return obj.isoformat()
        elif isinstance(obj, (np.integer, int)):
            return int(obj)
        elif isinstance(obj, (np.floating, float)):
            return float(obj)
        elif isinstance(obj, set):
            return list(obj)
        elif obj is None:
            return None
        else:
            try:
                json.dumps(obj)
                return obj
            except Exception:
                return str(obj)
    
    def load_lakehouse_data(self, 
                           column_usage_df: pd.DataFrame,
                           table_analysis_df: pd.DataFrame,
                           dataset_analysis_df: Optional[pd.DataFrame] = None,
                           relationships_df: Optional[pd.DataFrame] = None):
        """Load analysis data from lakehouse tables"""
        print("\n📥 Loading lakehouse analysis data...")
        
        self.column_usage_df = column_usage_df
        self.table_analysis_df = table_analysis_df
        self.dataset_analysis_df = dataset_analysis_df if dataset_analysis_df is not None else pd.DataFrame()
        self.relationships_df = relationships_df if relationships_df is not None else pd.DataFrame()
        
        print(f"  ✅ Loaded {len(self.column_usage_df)} column records")
        print(f"  ✅ Loaded {len(self.table_analysis_df)} table records")
        print(f"  ✅ Loaded {len(self.dataset_analysis_df)} dataset records")
        print(f"  ✅ Loaded {len(self.relationships_df)} relationship records")
    
    def map_datatype_to_tsql(self, pbi_datatype: str) -> str:
        """Map Power BI data type to T-SQL data type"""
        pbi_datatype = str(pbi_datatype).strip()
        
        if pbi_datatype in DATATYPE_MAPPING:
            return DATATYPE_MAPPING[pbi_datatype]
        
        pbi_lower = pbi_datatype.lower()
        
        if 'int' in pbi_lower or 'whole' in pbi_lower:
            return 'INT'
        elif 'NUMERIC' in pbi_lower or 'number' in pbi_lower or 'currency' in pbi_lower:
            return 'NUMERIC(18, 2)'
        elif 'double' in pbi_lower or 'float' in pbi_lower:
            return 'FLOAT'
        elif 'text' in pbi_lower or 'string' in pbi_lower:
            return 'VARCHAR(255)'
        elif 'date' in pbi_lower:
            if 'time' in pbi_lower:
                return 'DATETIME2'
            return 'DATE'
        elif 'bool' in pbi_lower:
            return 'BIT'
        else:
            return 'VARCHAR(255)'
    
    def prepare_dataset_migration(self, dataset_id: str) -> DatasetMigrationSpec:
        """Prepare specification for a single dataset"""
        print(f"\n🔄 Preparing specs for dataset: {dataset_id}")
        
        # Get dataset info
        if not self.dataset_analysis_df.empty:
            dataset_row = self.dataset_analysis_df[self.dataset_analysis_df['dataset_id'] == dataset_id]
            if dataset_row.empty:
                raise ValueError(f"Dataset {dataset_id} not found")
            dataset_info = dataset_row.iloc[0]
        else:
            dataset_tables = self.table_analysis_df[self.table_analysis_df['dataset_id'] == dataset_id]
            if dataset_tables.empty:
                raise ValueError(f"Dataset {dataset_id} not found")
            dataset_info = {
                'dataset_name': dataset_tables.iloc[0]['dataset_name'],
                'workspace_id': dataset_tables.iloc[0]['workspace_id'],
                'workspace_name': dataset_tables.iloc[0]['workspace_name']
            }
        
        dataset_name = dataset_info.get('dataset_name', 'Unknown')
        workspace_id = dataset_info.get('workspace_id', '')
        workspace_name = dataset_info.get('workspace_name', '')
        
        # Filter to used tables and columns
        used_tables_df = self.table_analysis_df[
            (self.table_analysis_df['dataset_id'] == dataset_id) &
            (self.table_analysis_df['is_used'] == True)
        ]
        
        used_columns_df = self.column_usage_df[
            (self.column_usage_df['dataset_id'] == dataset_id) &
            (self.column_usage_df['is_used'] == True)
        ]
        
        # Get excluded counts
        unused_tables = self.table_analysis_df[
            (self.table_analysis_df['dataset_id'] == dataset_id) &
            (self.table_analysis_df['is_used'] == False)
        ]
        
        unused_columns = self.column_usage_df[
            (self.column_usage_df['dataset_id'] == dataset_id) &
            (self.column_usage_df['is_used'] == False)
        ]
        
        excluded_tables = unused_tables['table_name'].unique().tolist()
        excluded_columns_count = len(unused_columns)
        
        print(f"  📊 Found {len(used_tables_df)} used tables")
        print(f"  📊 Found {len(used_columns_df)} used columns")
        print(f"  ⚠️  Excluding {len(excluded_tables)} unused tables")
        print(f"  ⚠️  Excluding {excluded_columns_count} unused columns")
        
        # Build table specifications
        table_specs = []
        
        for _, table_row in used_tables_df.iterrows():
            table_name = table_row['table_name']
            table_columns = used_columns_df[used_columns_df['table_name'] == table_name]
            
            # Build column specs
            column_specs = []
            for _, col_row in table_columns.iterrows():
                column_name = col_row['object_name']
                pbi_datatype = col_row.get('data_type', 'Unknown')
                tsql_datatype = self.map_datatype_to_tsql(pbi_datatype)
                
                # Check if column is in a relationship
                is_fk = False
                referenced_table = None
                referenced_column = None
                
                if not self.relationships_df.empty:
                    fk_rels = self.relationships_df[
                        (self.relationships_df['from_table'] == table_name) &
                        (self.relationships_df['from_column'] == column_name)
                    ]
                    
                    if not fk_rels.empty:
                        is_fk = True
                        rel = fk_rels.iloc[0]
                        referenced_table = rel.get('to_table', '')
                        referenced_column = rel.get('to_column', '')
                
                column_spec = ColumnSpec(
                    column_name=column_name,
                    data_type=pbi_datatype,
                    tsql_data_type=tsql_datatype,
                    is_nullable=True,
                    is_primary_key=False,
                    is_foreign_key=is_fk,
                    referenced_table=referenced_table,
                    referenced_column=referenced_column
                )
                column_specs.append(column_spec)
            
            # Get relationships for this table
            relationships_from = []
            relationships_to = []
            
            if not self.relationships_df.empty:
                rels_from = self.relationships_df[self.relationships_df['from_table'] == table_name]
                relationships_from = rels_from.to_dict('records')
                
                rels_to = self.relationships_df[self.relationships_df['to_table'] == table_name]
                relationships_to = rels_to.to_dict('records')
            
            # Usage metrics
            usage_metrics = {
                'measures_count': int(table_row.get('table_measure_count', 0)),
                'relationships_count': int(table_row.get('table_relationship_count', 0)),
                'dependencies_count': int(table_row.get('dependencies', 0))
            }
            
            table_spec = TableSpec(
                table_name=table_name,
                columns=column_specs,
                relationships_from=relationships_from,
                relationships_to=relationships_to,
                usage_metrics=usage_metrics
            )
            table_specs.append(table_spec)
        
        # Total relationship count for dataset
        total_relationships = len(self.relationships_df[
            self.relationships_df['dataset_id'] == dataset_id
        ]) if not self.relationships_df.empty else 0
        
        print(f"  ✅ Migration spec prepared with {len(table_specs)} tables")
        
        return DatasetMigrationSpec(
            dataset_id=dataset_id,
            dataset_name=dataset_name,
            workspace_id=workspace_id,
            workspace_name=workspace_name,
            tables=table_specs,
            excluded_tables=excluded_tables,
            excluded_columns=excluded_columns_count,
            excluded_measures=0,
            total_relationships=total_relationships
        )
    
    def export_migration_spec_to_json(self, migration_spec: DatasetMigrationSpec, output_path: str = ''):
        """Export migration spec to JSON file"""
        spec_dict = {
            'dataset_metadata': {
                'dataset_id': migration_spec.dataset_id,
                'dataset_name': migration_spec.dataset_name,
                'workspace_id': migration_spec.workspace_id,
                'workspace_name': migration_spec.workspace_name
            },
            'tables': [
                {
                    'table_name': table.table_name,
                    'columns': [
                        {
                            'column_name': col.column_name,
                            'original_data_type': col.data_type,
                            'tsql_data_type': col.tsql_data_type,
                            'is_nullable': col.is_nullable,
                            'is_primary_key': col.is_primary_key,
                            'is_foreign_key': col.is_foreign_key,
                            'referenced_table': col.referenced_table,
                            'referenced_column': col.referenced_column
                        }
                        for col in table.columns
                    ],
                    'relationships_from': table.relationships_from,
                    'relationships_to': table.relationships_to,
                    'usage_metrics': table.usage_metrics
                }
                for table in migration_spec.tables
            ],
            'exclusions': {
                'excluded_tables': migration_spec.excluded_tables,
                'excluded_columns_count': migration_spec.excluded_columns,
                'excluded_measures_count': migration_spec.excluded_measures
            },
            'metadata': {
                'total_relationships': migration_spec.total_relationships,
                'exported_at': datetime.now().isoformat(),
                'purpose': 'T-SQL CREATE TABLE generation for dataset migration'
            }
        }
        
        # Make JSON safe
        spec_dict_safe = self._make_json_safe(spec_dict)
        
        if output_path:
            # Make sure the directory exists
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            with open(output_path, "w") as f:
                json.dump(spec_dict_safe, f, indent=4)
            
            print(f"  ✅ Exported to {output_path}")
        
        return spec_dict_safe
    
    def generate_tsql_with_ai(self, migration_spec: DatasetMigrationSpec) -> str:
        """Generate T-SQL CREATE TABLE scripts using AI"""
        if not self.client:
            raise ValueError("AI client not initialized")
        
        prompt = self._build_tsql_generation_prompt(migration_spec)
        
        if self.agent_mode == "claude":
            response = self.client.messages.create(
                model="claude-sonnet-4-5",
                max_tokens=8000,
                messages=[{"role": "user", "content": prompt}]
            )
            return response.content[0].text
        
        elif self.agent_mode == "gemini":
            response = self.client.models.generate_content(
                model="gemini-2.0-flash-exp",
                contents=prompt,
                config=types.GenerateContentConfig(temperature=0.1)
            )
            return response.text
    
    def _build_tsql_generation_prompt(self, migration_spec: DatasetMigrationSpec) -> str:
        """Build the prompt for AI"""
        tables_section = []
        for table in migration_spec.tables:
            columns_info = []
            for col in table.columns:
                fk_info = f" (FK -> {col.referenced_table}.{col.referenced_column})" if col.is_foreign_key else ""
                columns_info.append(f"  - {col.column_name}: {col.tsql_data_type}{fk_info}")
            
            tables_section.append(f"""
Table: {table.table_name}
Columns:
{chr(10).join(columns_info)}
Usage: {table.usage_metrics['measures_count']} measures, {table.usage_metrics['relationships_count']} relationships
""")
        
        relationships_section = []
        for table in migration_spec.tables:
            for rel in table.relationships_from:
                relationships_section.append(
                    f"  - {rel.get('from_table', '')}.{rel.get('from_column', '')} -> "
                    f"{rel.get('to_table', '')}.{rel.get('to_column', '')} "
                    f"[{'Active' if rel.get('active', True) else 'Inactive'}]"
                )
        
        prompt = f"""You are an expert SQL developer specializing in dimensional modeling. Generate T-SQL CREATE TABLE scripts for Power BI dataset migration.

Dataset: {migration_spec.dataset_name}
Workspace: {migration_spec.workspace_name}

IMPORTANT CONSTRAINTS:
1. Use EXACT column names as provided (case-sensitive)
2. Use the EXACT T-SQL data types specified
3. Only include tables and columns listed below
4. Add comments for each table and column documenting the original Power BI context. for example:
``----------------------------------------------------------------------------------------------------
-- Table: States
-- Description: Represents geographical states, likely within the US, providing demographic and environmental data.
-- Usage in Power BI: 0 measures, 2 relationships
----------------------------------------------------------------------------------------------------
CREATE TABLE [States] (
    [Average Temperature ] FLOAT, -- The average temperature recorded for the state.
    [Flag] VARCHAR(255),         -- A flag or indicator associated with the state, possibly for categorization or status.
    [Population] FLOAT,          -- The total population of the state.
    [State] VARCHAR(255)         -- The name of the state.
);``

5. Do NOT include PRIMARY KEY constraints
6. Do NOT include FOREIGN KEY constraints
7. Do NOT include INDEX definitions
8. Return the T-SQL script as a valid SQL file ready to be saved in lakehouse as .sql

EXCLUSIONS (already filtered):
- {len(migration_spec.excluded_tables)} unused tables excluded
- {migration_spec.excluded_columns} unused columns excluded

TABLES TO CREATE:
{chr(10).join(tables_section)}

RELATIONSHIPS (for reference):
{chr(10).join(relationships_section) if relationships_section else '  - No relationships defined'}

Generate the complete T-SQL migration script now:"""
        
        return prompt


# In[8]:


class MCodeExtractor:
    """Extracts M code expressions from lakehouse tables"""
    
    def __init__(self, spark: Optional[SparkSession] = None):
        self.spark = spark if spark else SparkSession.builder.getOrCreate()
        self.expressions_df = pd.DataFrame()
        print("✅ M Code Extractor initialized")
    
    def load_expressions_from_lakehouse(self, table_name: str = "dataset_expressions") -> pd.DataFrame:
        """Load M code expressions from lakehouse table"""
        print(f"\n📥 Loading M code expressions from: {table_name}")
        
        try:
            expressions_spark = self.spark.table(table_name)
            self.expressions_df = expressions_spark.toPandas()
            
            print(f"  ✅ Loaded {len(self.expressions_df)} expression records")
            
            if not self.expressions_df.empty:
                unique_datasets = self.expressions_df['dataset_id'].nunique()
                unique_tables = self.expressions_df['table_name'].nunique()
                print(f"  📊 Unique datasets: {unique_datasets}")
                print(f"  📊 Unique tables: {unique_tables}")
            
            return self.expressions_df
        except Exception as e:
            print(f"  ❌ Error: {e}")
            print(f"  ℹ️  Table '{table_name}' may not exist")
            return pd.DataFrame()
    
    def extract_by_dataset(self, dataset_id: str) -> Optional[MCodeExtractionResult]:
        """Extract all M code expressions for a specific dataset"""
        print(f"\n🔍 Extracting M code for dataset: {dataset_id}")
        
        if self.expressions_df.empty:
            print("  ⚠️  No expressions data loaded")
            return None
        
        dataset_expressions = self.expressions_df[
            self.expressions_df['dataset_id'] == dataset_id
        ].copy()
        
        if dataset_expressions.empty:
            print(f"  ⚠️  No expressions found")
            return None
        
        first_row = dataset_expressions.iloc[0]
        dataset_name = first_row.get('dataset_name', 'Unknown')
        workspace_name = first_row.get('workspace_name', 'Unknown')
        workspace_id = first_row.get('workspace_id', '')
        
        print(f"  📊 Dataset: {dataset_name}")
        print(f"  📊 Workspace: {workspace_name}")
        print(f"  📊 Found {len(dataset_expressions)} expressions")
        
        expressions = []
        for _, row in dataset_expressions.iterrows():
            expression = MCodeExpression(
                dataset_id=dataset_id,
                dataset_name=dataset_name,
                workspace_id=workspace_id,
                workspace_name=workspace_name,
                table_name=row.get('table_name', ''),
                expression=row.get('expression', ''),
                expression_type=row.get('expression_type', 'table'),
                object_name=row.get('object_name'),
                column_name=row.get('column_name')
            )
            expressions.append(expression)
        
        tables_with_expressions = dataset_expressions['table_name'].unique().tolist()
        
        return MCodeExtractionResult(
            dataset_id=dataset_id,
            dataset_name=dataset_name,
            workspace_name=workspace_name,
            expressions=expressions,
            total_expressions=len(expressions),
            tables_with_expressions=tables_with_expressions,
            extraction_timestamp=datetime.now().isoformat()
        )
    
    def get_datasets_with_expressions(self) -> pd.DataFrame:
        """Get summary of all datasets that have M code expressions"""
        if self.expressions_df.empty:
            return pd.DataFrame()
        
        summary = self.expressions_df.groupby([
            'dataset_id',
            'dataset_name',
            'workspace_name'
        ]).agg({
            'table_name': 'nunique',
            'expression': 'count'
        }).reset_index()
        
        summary.columns = [
            'dataset_id',
            'dataset_name',
            'workspace_name',
            'unique_tables',
            'total_expressions'
        ]
        
        return summary


# In[9]:


class MCodeToSQLIntegration:
    """Integration module to connect M Code Extractor with AI-based SQL generation"""
    
    def __init__(self, m_extractor: MCodeExtractor, tsql_prep: TSQLMigrationPrep):
        self.m_extractor = m_extractor
        self.tsql_prep = tsql_prep
        print("✅ M Code to SQL Integration initialized")
    
    def build_m_to_sql_prompt(self,
                              m_code_expression: MCodeExpression,
                              target_table_name: str,
                              additional_context: Optional[str] = None) -> str:
        """Build AI prompt for M code to SQL transformation"""
        prompt = f"""You are an expert in Power Query M language and T-SQL. Transform the following M code into an equivalent T-SQL SELECT statement.

## CONTEXT
{m_code_expression.get_context()}

## SOURCE M CODE
```m
{m_code_expression.expression}
```

## TARGET SQL TABLE
Data is loaded into: `{target_table_name}`

## TRANSFORMATION REQUIREMENTS
1. Convert M operations to SQL:
   - `Table.SelectColumns()` → `SELECT` clause
   - `Table.SelectRows()` → `WHERE` clause
   - `Table.ReplaceValue()` → `REPLACE()` function
   - `Table.AddColumn()` → Calculated column in `SELECT`
   - `Table.RenameColumns()` → Column `AS` aliases
   - `Table.TransformColumnTypes()` → `CAST()` or schema

2. Handle M patterns:
   - `[ColumnName]` → Standard SQL column names
   - `each` keyword → SQL expressions
   - M operators (`&`, `<>`) → SQL equivalents

3. Output:
   - Valid T-SQL for SQL Server 2019+ / Fabric DW
   - Proper quoting for special column names
   - Comments explaining transformations
   - Preserve column names exactly
   - Valid .sql for saving in lakehouse files

4. Important:
   - Assume source data is ALREADY in target table
   - Focus ONLY on transformation logic
   - If M code has data source operations, note as prerequisites

{additional_context if additional_context else ''}

## OUTPUT
Generate the T-SQL SELECT statement:
"""
        return prompt
    
    def generate_sql_from_m_code(self,
                                 dataset_id: str,
                                 table_name: Optional[str] = None,
                                 target_table_prefix: str = "stg_") -> Dict:
        """Generate SQL transformations for M code expressions"""
        print(f"\n🔄 Generating SQL transformations for dataset: {dataset_id}")
        
        if not self.tsql_prep.client:
            print("  ⚠️  AI client not initialized")
            return {}
        
        extraction_result = self.m_extractor.extract_by_dataset(dataset_id)
        
        if not extraction_result:
            return {}
        
        results = {
            'dataset_id': dataset_id,
            'dataset_name': extraction_result.dataset_name,
            'workspace_name': extraction_result.workspace_name,
            'transformations': []
        }
        
        tables_to_process = [table_name] if table_name else extraction_result.tables_with_expressions
        
        for tbl in tables_to_process:
            print(f"\n  🔹 Processing table: {tbl}")
            
            table_expressions = [
                exp for exp in extraction_result.expressions
                if exp.table_name == tbl
            ]
            
            for expr in table_expressions:
                target_table = f"{target_table_prefix}{tbl}"
                prompt = self.build_m_to_sql_prompt(expr, target_table)
                
                try:
                    if self.tsql_prep.agent_mode == "claude":
                        response = self.tsql_prep.client.messages.create(
                            model="claude-sonnet-4-5",
                            max_tokens=4000,
                            messages=[{"role": "user", "content": prompt}]
                        )
                        generated_sql = response.content[0].text
                    
                    elif self.tsql_prep.agent_mode == "gemini":
                        response = self.tsql_prep.client.models.generate_content(
                            model="gemini-2.0-flash-exp",
                            contents=prompt,
                            config=types.GenerateContentConfig(temperature=0.1)
                        )
                        generated_sql = response.text
                    
                    results['transformations'].append({
                        'table_name': tbl,
                        'expression_type': expr.expression_type,
                        'object_name': expr.object_name,
                        'original_m_code': expr.expression,
                        'generated_sql': generated_sql,
                        'target_table': target_table,
                        'status': 'success'
                    })
                    
                    print(f"    ✅ Generated SQL for {expr.expression_type}")
                    
                except Exception as e:
                    print(f"    ❌ Error: {e}")
                    results['transformations'].append({
                        'table_name': tbl,
                        'expression_type': expr.expression_type,
                        'object_name': expr.object_name,
                        'original_m_code': expr.expression,
                        'generated_sql': None,
                        'target_table': target_table,
                        'status': 'failed',
                        'error': str(e)
                    })
        
        print(f"\n  ✅ Completed {len(results['transformations'])} transformations")
        
        return results


# In[10]:


def save_sql_to_lakehouse_file(dataset_id: str, sql_content: str) -> bool:
    """
    Save SQL CREATE TABLE statements to a .sql file in the lakehouse Files directory.
    
    Args:
        dataset_id: The dataset ID to use for folder and file naming
        sql_content: The SQL content to save
    
    Returns:
        bool: True if successful, False otherwise
    """
    if not sql_content or not sql_content.strip():
        print(f"  ⚠️  No SQL content to save for dataset {dataset_id}")
        return False
    
    try:
        # Construct the file path
        base_path = "/lakehouse/default/Files/sql_scripts"
        dataset_folder = f"{base_path}/{dataset_id}"
        sql_filename = f"{dataset_id}_create_tables.sql"
        sql_filepath = f"{dataset_folder}/{sql_filename}"
        
        # Create directory structure if it doesn't exist
        os.makedirs(dataset_folder, exist_ok=True)
        
        # Write SQL content to file
        with open(sql_filepath, 'w', encoding='utf-8') as f:
            f.write(sql_content)
        
        print(f"  ✅ SQL file saved to: {sql_filepath}")
        return True
        
    except Exception as e:
        print(f"  ❌ Error saving SQL file for dataset {dataset_id}: {str(e)}")
        return False


def save_m_to_sql_to_lakehouse_files(dataset_id: str, m_to_sql_result: Dict) -> int:
    """
    Save M-to-SQL transformation scripts to .sql files in the lakehouse Files directory.
    
    Args:
        dataset_id: The dataset ID to use for folder and file naming
        m_to_sql_result: Dictionary containing transformations from generate_sql_from_m_code
    
    Returns:
        int: Number of files successfully saved
    """
    if not m_to_sql_result or 'transformations' not in m_to_sql_result:
        print(f"  ⚠️  No M-to-SQL transformations to save for dataset {dataset_id}")
        return 0
    
    transformations = m_to_sql_result.get('transformations', [])
    if not transformations:
        print(f"  ⚠️  No transformations found for dataset {dataset_id}")
        return 0
    
    dataset_name = m_to_sql_result.get('dataset_name', 'Unknown')
    workspace_name = m_to_sql_result.get('workspace_name', 'Unknown')
    
    try:
        # Construct the base path
        base_path = "/lakehouse/default/Files/sql_scripts"
        dataset_folder = f"{base_path}/{dataset_id}/m_to_sql"
        
        # Create directory structure if it doesn't exist
        os.makedirs(dataset_folder, exist_ok=True)
        
        saved_count = 0
        
        for transformation in transformations:
            # Skip failed transformations
            if transformation.get('status') != 'success' or not transformation.get('generated_sql'):
                continue
            
            table_name = transformation.get('table_name', 'unknown')
            expression_type = transformation.get('expression_type', 'unknown')
            object_name = transformation.get('object_name', '')
            generated_sql = transformation.get('generated_sql', '')
            original_m_code = transformation.get('original_m_code', '')
            target_table = transformation.get('target_table', '')
            
            # Create safe filename (sanitize table name and expression type)
            safe_table_name = re.sub(r'[^\w\s-]', '', table_name).strip().replace(' ', '_')
            safe_expression_type = re.sub(r'[^\w\s-]', '', expression_type).strip().replace(' ', '_')
            
            # Build filename
            if object_name:
                safe_object_name = re.sub(r'[^\w\s-]', '', object_name).strip().replace(' ', '_')
                sql_filename = f"{safe_table_name}_{safe_expression_type}_{safe_object_name}.sql"
            else:
                sql_filename = f"{safe_table_name}_{safe_expression_type}.sql"
            
            sql_filepath = f"{dataset_folder}/{sql_filename}"
            
            # Build file content with header comments
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            file_content = f"""-- ====================================================================================
-- M-to-SQL Transformation Script
-- ====================================================================================
-- Dataset: {dataset_name}
-- Workspace: {workspace_name}
-- Dataset ID: {dataset_id}
-- Table: {table_name}
-- Expression Type: {expression_type}
-- Object Name: {object_name if object_name else 'N/A'}
-- Target Table: {target_table}
-- Generated: {timestamp}
-- ====================================================================================
-- Original M Code Reference:
-- {original_m_code[:200]}{'...' if len(original_m_code) > 200 else ''}
-- ====================================================================================

{generated_sql}
"""
            
            # Write SQL content to file
            with open(sql_filepath, 'w', encoding='utf-8') as f:
                f.write(file_content)
            
            saved_count += 1
            print(f"    ✅ Saved: {sql_filename}")
        
        if saved_count > 0:
            print(f"  ✅ Saved {saved_count} M-to-SQL transformation file(s) to: {dataset_folder}")
        else:
            print(f"  ⚠️  No successful transformations to save for dataset {dataset_id}")
        
        return saved_count
        
    except Exception as e:
        print(f"  ❌ Error saving M-to-SQL files for dataset {dataset_id}: {str(e)}")
        return 0


# In[11]:


def run_complete_migration(
    agent_mode: str = 'gemini',
    api_key: str = '',
    dataset_ids: Optional[List[str]] = None,
    process_all_datasets: bool = True,
    generate_create_tables: bool = True,
    generate_m_to_sql: bool = False,
    export_json: bool = True,
    save_to_lakehouse: bool = True
):
    """
    Complete migration workflow
    
    Args:
        agent_mode: 'claude' or 'gemini'
        api_key: API key for AI provider
        dataset_ids: Specific datasets to process (optional)
        process_all_datasets: Process all datasets (default: True)
        generate_create_tables: Generate CREATE TABLE scripts (default: True)
        generate_m_to_sql: Generate M-to-SQL transformations (default: False)
        export_json: Export specs to JSON (default: True)
        save_to_lakehouse: Save results to lakehouse (default: True)
    
    Returns:
        Dictionary with all results and summary
    """
    
    print(f"\n{'='*80}")
    print(f"🚀 Starting Complete Fabric Migration Workflow")
    print(f"   AI Provider: {agent_mode.upper()}")
    print(f"   CREATE TABLE: {'Yes' if generate_create_tables and api_key else 'No'}")
    print(f"   M to SQL: {'Yes' if generate_m_to_sql and api_key else 'No'}")
    print(f"{'='*80}\n")
    
    # Load lakehouse tables
    print("📊 Step 1: Loading lakehouse tables...")
    data_context_pd = spark.table("ai_dataset_context").toPandas()
    relationships_pd = spark.table("dataset_relationships").toPandas()
    objects_spark = spark.read.table("ai_object_features")
    
    print(f"  ✅ Loaded {len(data_context_pd)} datasets from context")
    
    # Prepare table analysis
    print("\n🔧 Step 2: Preparing table analysis...")
    tables = objects_spark.groupBy([
        'workspace_id',
        'workspace_name',
        'dataset_id',
        'dataset_name',
        'table_name'
    ]).agg(
        F.mean('usage_score').alias('usage_score'),
        F.first('table_measure_count').alias('table_measure_count'),
        F.first('table_column_count').alias('table_column_count'),
        F.first('table_relationship_count').alias('table_relationship_count'),
        F.first('table_is_isolated').alias('table_is_isolated'),
        F.sum('used_by_dependencies').alias('dependencies')
    ).withColumn(
        'is_used',
        F.when(F.col('usage_score') > 0, True).otherwise(False)
    )
    
    tables_pd = tables.toPandas()
    
    # Filter columns
    print("\n🔧 Step 3: Filtering column data...")
    columns = objects_spark[objects_spark['object_type'] == 'column']
    columns_pd = columns.toPandas()
    
    print(f"  ✅ Prepared {len(tables_pd)} table records")
    print(f"  ✅ Prepared {len(columns_pd)} column records")
    
    # Determine datasets to process
    if process_all_datasets:
        datasets_to_process = data_context_pd['dataset_id'].unique().tolist()
    elif dataset_ids:
        datasets_to_process = dataset_ids
    else:
        raise ValueError("Either set process_all_datasets=True or provide dataset_ids")
    
    print(f"\n📋 Total datasets to process: {len(datasets_to_process)}")
    
    # Initialize components
    print("\n⚙️  Step 4: Initializing migration components...")
    tsql_prep = TSQLMigrationPrep(api_key=api_key, agent_mode=agent_mode)
    
    tsql_prep.load_lakehouse_data(
        column_usage_df=columns_pd,
        table_analysis_df=tables_pd,
        dataset_analysis_df=data_context_pd,
        relationships_df=relationships_pd
    )
    
    # Initialize M code extractor if needed
    m_extractor = None
    integration = None
    
    if generate_m_to_sql:
        m_extractor = MCodeExtractor(spark)
        m_extractor.load_expressions_from_lakehouse("dataset_expressions")
        integration = MCodeToSQLIntegration(m_extractor, tsql_prep)
    
    # Process datasets
    print(f"\n{'='*80}")
    print(f"🔄 Processing {len(datasets_to_process)} datasets...")
    print(f"{'='*80}\n")
    
    all_results = []
    successful_count = 0
    failed_count = 0
    
    for idx, dataset_id in enumerate(datasets_to_process, 1):
        try:
            print(f"\n{'─'*80}")
            print(f"📦 [{idx}/{len(datasets_to_process)}] Processing: {dataset_id}")
            print(f"{'─'*80}")
            
            # Prepare migration spec
            dataset_meta = tsql_prep.prepare_dataset_migration(dataset_id)
            
            result = {
                'dataset_id': dataset_id,
                'dataset_name': dataset_meta.dataset_name,
                'workspace_name': dataset_meta.workspace_name,
                'migration_spec': dataset_meta,
                'json_spec': None,
                'create_table_sql': None,
                'm_to_sql_transformations': None,
                'status': 'success',
                'error': None,
                'timestamp': datetime.now().isoformat(),
                'tables_count': len(dataset_meta.tables),
                'columns_count': sum(len(t.columns) for t in dataset_meta.tables)
            }
            
            # Export to JSON
            if export_json:
                json_spec = tsql_prep.export_migration_spec_to_json(
                    dataset_meta,
                    output_path=f"/lakehouse/default/Files/migration_specs/{dataset_id}.json"
                )
                result['json_spec'] = json_spec
            
            # Generate CREATE TABLE scripts
            if generate_create_tables and api_key:
                print(f"\n🤖 Generating CREATE TABLE scripts...")
                create_table_sql = tsql_prep.generate_tsql_with_ai(dataset_meta)
                result['create_table_sql'] = create_table_sql
                print(f"  ✅ CREATE TABLE scripts generated")

                # Save SQL to file in lakehouse
                if create_table_sql:
                    save_sql_to_lakehouse_file(dataset_id, create_table_sql)
            
            # Generate M-to-SQL transformations
            if generate_m_to_sql and api_key and integration:
                print(f"\n🤖 Generating M-to-SQL transformations...")
                m_to_sql_result = integration.generate_sql_from_m_code(
                    dataset_id=dataset_id,
                    target_table_prefix="stg_"
                )
                result['m_to_sql_transformations'] = m_to_sql_result
                print(f"  ✅ M-to-SQL transformations generated")
                
                # Save M-to-SQL scripts to files
                if m_to_sql_result:
                    save_m_to_sql_to_lakehouse_files(dataset_id, m_to_sql_result)
            
            all_results.append(result)
            successful_count += 1
            
            print(f"\n✅ Dataset {dataset_meta.dataset_name} processed successfully")
            
            # Small delay for API rate limits
            if idx < len(datasets_to_process):
                time.sleep(1)
        
        except Exception as e:
            failed_count += 1
            error_result = {
                'dataset_id': dataset_id,
                'dataset_name': 'Unknown',
                'workspace_name': 'Unknown',
                'migration_spec': None,
                'json_spec': None,
                'create_table_sql': None,
                'm_to_sql_transformations': None,
                'status': 'failed',
                'error': str(e),
                'timestamp': datetime.now().isoformat(),
                'tables_count': 0,
                'columns_count': 0
            }
            all_results.append(error_result)
            print(f"\n❌ Error processing dataset: {e}")
    
    # Save results to lakehouse
    if save_to_lakehouse and all_results:
        print(f"\n{'='*80}")
        print(f"💾 Saving results to lakehouse")
        print(f"{'='*80}\n")
        
        # CREATE TABLE results
        if generate_create_tables:
            create_table_data = []
            for result in all_results:
                create_table_data.append({
                    'dataset_id': result['dataset_id'],
                    'dataset_name': result['dataset_name'],
                    'workspace_name': result['workspace_name'],
                    'status': result['status'],
                    'tables_count': result['tables_count'],
                    'columns_count': result['columns_count'],
                    'create_table_sql': result['create_table_sql'] if result['create_table_sql'] else '',
                    'error_message': result['error'] if result['error'] else '',
                    'timestamp': result['timestamp']
                })
            
            create_df = pd.DataFrame(create_table_data)
            spark.createDataFrame(create_df).write.mode("overwrite").saveAsTable("tsql_migration_results")
            print(f"  ✅ CREATE TABLE results saved to tsql_migration_results")
        
        # M-to-SQL results
        if generate_m_to_sql:
            m_to_sql_data = []
            for result in all_results:
                if result['m_to_sql_transformations']:
                    for t in result['m_to_sql_transformations'].get('transformations', []):
                        m_to_sql_data.append({
                            'dataset_id': result['dataset_id'],
                            'dataset_name': result['dataset_name'],
                            'workspace_name': result['workspace_name'],
                            'table_name': t['table_name'],
                            'expression_type': t['expression_type'],
                            'original_m_code': t['original_m_code'],
                            'generated_sql': t['generated_sql'] if t['generated_sql'] else '',
                            'target_table': t['target_table'],
                            'status': t['status'],
                            'timestamp': result['timestamp']
                        })
            
            if m_to_sql_data:
                m_df = pd.DataFrame(m_to_sql_data)
                spark.createDataFrame(m_df).write.mode("overwrite").saveAsTable("m_to_sql_transformations")
                print(f"  ✅ M-to-SQL results saved to m_to_sql_transformations")
    
    # Summary
    print(f"\n{'='*80}")
    print(f"📊 MIGRATION SUMMARY")
    print(f"{'='*80}")
    print(f"  ✅ Total Datasets: {len(datasets_to_process)}")
    print(f"  ✅ Successful: {successful_count}")
    print(f"  ❌ Failed: {failed_count}")
    print(f"  📊 Total Tables: {sum(r['tables_count'] for r in all_results)}")
    print(f"  📊 Total Columns: {sum(r['columns_count'] for r in all_results)}")
    print(f"{'='*80}\n")
    
    return {
        'all_results': all_results,
        'summary': {
            'total_datasets': len(datasets_to_process),
            'successful': successful_count,
            'failed': failed_count,
            'total_tables': sum(r['tables_count'] for r in all_results),
            'total_columns': sum(r['columns_count'] for r in all_results)
        }
    }


# In[12]:


# Configuration
gemini_key = ""
claude_key = ""


# ## Usage Examples
# 
# ### Example 1: CREATE TABLE Scripts Only (No M Code)

# In[ ]:


# Run CREATE TABLE generation only
results = run_complete_migration(
    agent_mode='gemini',
    api_key=gemini_key,
    process_all_datasets=True,
    generate_create_tables=True,
    generate_m_to_sql=False,  # Don't process M code
    export_json=True,
    save_to_lakehouse=True
)


# ### Example 2: Complete Migration (CREATE TABLE + M-to-SQL)

# In[16]:


# Run CREATE TABLE generation only
results = run_complete_migration(
    agent_mode='claude',
    api_key=claude_key,
    process_all_datasets=True,
    generate_create_tables=True,
    generate_m_to_sql=True,
    export_json=True,
    save_to_lakehouse=True
)


# In[14]:


display(fabric.list_datasets())


# ### Example 3: Specific Datasets Only

# In[13]:


# Process specific datasets
specific_datasets = [
    "4b08eee3-3c01-441c-b7bd-0ec50e7aa12f"
]

results = run_complete_migration(
    agent_mode='claude',
    api_key=claude_key,
    dataset_ids=specific_datasets,
    process_all_datasets=False,
    generate_create_tables=True,
    generate_m_to_sql=True,
    export_json=True,
    save_to_lakehouse=True
)


# ### View Results

# In[17]:


# View summary
print("\n📊 Summary:")
print(results['summary'])

# View specific dataset result
if results['all_results']:
    first_result = results['all_results'][0]
    print(f"\nDataset: {first_result['dataset_name']}")
    print(f"Status: {first_result['status']}")
    print(f"Tables: {first_result['tables_count']}")
    print(f"Columns: {first_result['columns_count']}")
    
    if first_result['create_table_sql']:
        print("\n📜 CREATE TABLE SQL (first 500 chars):")
        print(first_result['create_table_sql'][:500] + "...")

