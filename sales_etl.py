#!/usr/bin/env python3
"""
Sales Data ETL Pipeline

This script performs Extract, Transform, Load operations on sales data:
1. Extract: Read data from a CSV file
2. Transform: Clean and process the data
3. Load: Insert the data into a SQL database

Author: Claude
Date: April 29, 2025
"""

import os
import pandas as pd
import sqlite3
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("etl_pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
INPUT_FILE = "sales_data.csv"
DATABASE_FILE = "sales_database.db"
TABLE_NAME = "sales_summary"

def extract(file_path):
    """
    Extract data from CSV file
    
    Args:
        file_path (str): Path to the CSV file
        
    Returns:
        pandas.DataFrame: Raw data from CSV
    """
    logger.info(f"Extracting data from {file_path}")
    
    if not os.path.exists(file_path):
        logger.error(f"Input file {file_path} not found!")
        raise FileNotFoundError(f"Input file {file_path} not found!")
    
    try:
        # Read the CSV file into a pandas DataFrame
        df = pd.read_csv(file_path)
        logger.info(f"Successfully extracted {len(df)} rows from {file_path}")
        return df
    except Exception as e:
        logger.error(f"Error extracting data from {file_path}: {str(e)}")
        raise

def transform(df):
    """
    Transform and clean the data
    
    Args:
        df (pandas.DataFrame): Raw data
        
    Returns:
        pandas.DataFrame: Transformed data
    """
    logger.info("Starting data transformation")
    
    try:
        # Store original row count
        original_count = len(df)
        
        # 1. Remove rows with missing values
        df = df.dropna()
        missing_rows = original_count - len(df)
        logger.info(f"Removed {missing_rows} rows with missing values")
        
        # 2. Convert order_date to standard format (YYYY-MM-DD)
        df['order_date'] = pd.to_datetime(df['order_date']).dt.strftime('%Y-%m-%d')
        logger.info("Converted order_date to YYYY-MM-DD format")
        
        # 3. Create new column: total_price = quantity_sold × price_per_unit
        df['total_price'] = df['quantity_sold'] * df['price_per_unit']
        df['total_price'] = df['total_price'].round(2)  # Round to 2 decimal places
        logger.info("Created new column 'total_price'")
        
        # 4. Additional transformations:
        # Convert product names to title case for consistency
        df['product_name'] = df['product_name'].str.title()
        
        logger.info(f"Transformation complete. Final dataset has {len(df)} rows")
        return df
    
    except Exception as e:
        logger.error(f"Error transforming data: {str(e)}")
        raise

def create_database_and_table(db_path, table_name):
    """
    Create SQLite database and table if they don't exist
    
    Args:
        db_path (str): Path to the SQLite database
        table_name (str): Name of the table to create
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Create table if it doesn't exist
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            order_id TEXT PRIMARY KEY,
            product_name TEXT,
            quantity_sold INTEGER,
            price_per_unit REAL,
            order_date TEXT,
            total_price REAL
        )
        """
        cursor.execute(create_table_sql)
        conn.commit()
        logger.info(f"Database and table '{table_name}' created/verified successfully")
        return conn
    
    except Exception as e:
        logger.error(f"Error creating database and table: {str(e)}")
        if conn:
            conn.close()
        raise

def load(df, db_path, table_name):
    """
    Load transformed data into SQL database
    
    Args:
        df (pandas.DataFrame): Transformed data
        db_path (str): Path to the SQLite database
        table_name (str): Name of the table to load data into
    """
    logger.info(f"Starting data load into {db_path}, table: {table_name}")
    
    try:
        # Create database and table
        conn = create_database_and_table(db_path, table_name)
        cursor = conn.cursor()
        
        # Check for existing records to avoid duplicates
        cursor.execute(f"SELECT order_id FROM {table_name}")
        existing_order_ids = [row[0] for row in cursor.fetchall()]
        
        # Only insert new records (not already in the database)
        new_records = df[~df['order_id'].isin(existing_order_ids)]
        
        if len(new_records) == 0:
            logger.info("No new records to insert")
            conn.close()
            return
        
        # Insert data into the table
        new_records.to_sql(table_name, conn, if_exists='append', index=False)
        
        logger.info(f"Successfully loaded {len(new_records)} new records into {table_name}")
        conn.close()
    
    except Exception as e:
        logger.error(f"Error loading data into database: {str(e)}")
        if 'conn' in locals() and conn:
            conn.close()
        raise

def run_etl_pipeline():
    """
    Run the complete ETL pipeline
    """
    logger.info("Starting ETL pipeline")
    start_time = datetime.now()
    
    try:
        # Extract
        raw_data = extract(INPUT_FILE)
        
        # Transform
        transformed_data = transform(raw_data)
        
        # Load
        load(transformed_data, DATABASE_FILE, TABLE_NAME)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"ETL pipeline completed successfully in {duration:.2f} seconds")
    
    except Exception as e:
        logger.error(f"ETL pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    run_etl_pipeline()