#!/usr/bin/env python3
"""
Generate Sample Sales Data

This script creates a sample CSV file with sales data to use in the ETL pipeline.

Author: Chandu Kalluru
Date: April 29, 2025
"""

import pandas as pd
import numpy as np
import uuid
from datetime import datetime, timedelta
import random

# Configuration
OUTPUT_FILE = "sales_data.csv"
NUM_RECORDS = 1000

def generate_sample_data(num_records):
    """
    Generate sample sales data
    
    Args:
        num_records (int): Number of records to generate
        
    Returns:
        pandas.DataFrame: Generated data
    """
    # List of sample products with their price ranges
    products = [
        {"name": "Laptop", "min_price": 799.99, "max_price": 1999.99},
        {"name": "Smartphone", "min_price": 299.99, "max_price": 1299.99},
        {"name": "Headphones", "min_price": 49.99, "max_price": 349.99},
        {"name": "Monitor", "min_price": 149.99, "max_price": 699.99},
        {"name": "Keyboard", "min_price": 29.99, "max_price": 199.99},
        {"name": "Mouse", "min_price": 14.99, "max_price": 99.99},
        {"name": "tablet", "min_price": 199.99, "max_price": 899.99},
        {"name": "printer", "min_price": 89.99, "max_price": 399.99},
        {"name": "usb cable", "min_price": 9.99, "max_price": 29.99},
        {"name": "external hard drive", "min_price": 59.99, "max_price": 299.99}
    ]
    
    # Create empty lists for each column
    order_ids = []
    product_names = []
    quantities = []
    prices = []
    order_dates = []
    
    # Generate current date for reference
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)  # One year of data
    
    # Generate records
    for _ in range(num_records):
        # Generate order ID (UUID format)
        order_id = str(uuid.uuid4())
        
        # Select random product
        product = random.choice(products)
        product_name = product["name"]
        
        # Generate random quantity (1-10)
        quantity = random.randint(1, 10)
        
        # Generate random price within product's range
        price = round(random.uniform(product["min_price"], product["max_price"]), 2)
        
        # Generate random date within the past year
        days_offset = random.randint(0, 365)
        order_date = (end_date - timedelta(days=days_offset)).strftime('%m/%d/%Y')
        
        # Add to lists
        order_ids.append(order_id)
        product_names.append(product_name)
        quantities.append(quantity)
        prices.append(price)
        order_dates.append(order_date)
    
    # Create DataFrame
    df = pd.DataFrame({
        'order_id': order_ids,
        'product_name': product_names,
        'quantity_sold': quantities,
        'price_per_unit': prices,
        'order_date': order_dates
    })
    
    # Add some null values for demonstration of data cleaning
    # Set ~5% of values to NaN
    mask = np.random.random(size=df.shape) < 0.05
    df = df.mask(mask)
    
    return df

def save_to_csv(df, output_file):
    """
    Save DataFrame to CSV file
    
    Args:
        df (pandas.DataFrame): Data to save
        output_file (str): Output file path
    """
    df.to_csv(output_file, index=False)
    print(f"Sample data saved to {output_file}")
    print(f"Generated {len(df)} records with {df.isna().sum().sum()} null values")

if __name__ == "__main__":
    print("Generating sample sales data...")
    sample_data = generate_sample_data(NUM_RECORDS)
    save_to_csv(sample_data, OUTPUT_FILE)
    print("Sample data generation complete!")