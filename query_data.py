#!/usr/bin/env python3
"""
Query and Analyze Sales Data

This script retrieves data from the SQLite database and performs basic analytics.

Author: Chandu Kalluru
Date: April 29, 2025
"""

import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import os

# Configuration
DATABASE_FILE = "sales_database.db"
TABLE_NAME = "sales_summary"
REPORTS_DIR = "reports"

def connect_to_database(db_path):
    """
    Connect to SQLite database
    
    Args:
        db_path (str): Path to the SQLite database
        
    Returns:
        sqlite3.Connection: Database connection
    """
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database file {db_path} not found!")
    
    return sqlite3.connect(db_path)

def run_sales_queries():
    """
    Run various analysis queries on the sales data
    """
    # Create reports directory if it doesn't exist
    if not os.path.exists(REPORTS_DIR):
        os.makedirs(REPORTS_DIR)
    
    try:
        # Connect to the database
        conn = connect_to_database(DATABASE_FILE)
        
        # 1. Total sales by product
        product_sales_query = f"""
        SELECT 
            product_name, 
            SUM(quantity_sold) as total_quantity, 
            SUM(total_price) as total_revenue,
            ROUND(AVG(price_per_unit), 2) as average_price
        FROM {TABLE_NAME}
        GROUP BY product_name
        ORDER BY total_revenue DESC
        """
        product_sales = pd.read_sql_query(product_sales_query, conn)
        print("\n--- Total Sales by Product ---")
        print(product_sales)
        
        # Save to CSV
        product_sales.to_csv(f"{REPORTS_DIR}/product_sales_summary.csv", index=False)
        
        # 2. Monthly sales trend
        monthly_trend_query = f"""
        SELECT 
            substr(order_date, 1, 7) as month,
            COUNT(*) as order_count,
            SUM(total_price) as monthly_revenue
        FROM {TABLE_NAME}
        GROUP BY month
        ORDER BY month
        """
        monthly_trend = pd.read_sql_query(monthly_trend_query, conn)
        print("\n--- Monthly Sales Trend ---")
        print(monthly_trend)
        
        # Save to CSV
        monthly_trend.to_csv(f"{REPORTS_DIR}/monthly_sales_trend.csv", index=False)
        
        # 3. Top selling products by quantity
        top_products_query = f"""
        SELECT 
            product_name,
            SUM(quantity_sold) as total_quantity
        FROM {TABLE_NAME}
        GROUP BY product_name
        ORDER BY total_quantity DESC
        LIMIT 5
        """
        top_products = pd.read_sql_query(top_products_query, conn)
        print("\n--- Top 5 Selling Products by Quantity ---")
        print(top_products)
        
        # 4. Generate some basic visualizations
        create_visualizations(product_sales, monthly_trend)
        
        # 5. Get database statistics
        stats_query = f"""
        SELECT 
            COUNT(*) as total_orders,
            COUNT(DISTINCT product_name) as unique_products,
            SUM(total_price) as total_revenue,
            AVG(total_price) as average_order_value,
            MIN(order_date) as earliest_date,
            MAX(order_date) as latest_date
        FROM {TABLE_NAME}
        """
        stats = pd.read_sql_query(stats_query, conn)
        print("\n--- Database Statistics ---")
        print(stats)
        
        conn.close()
        print(f"\nAnalysis reports saved to '{REPORTS_DIR}' directory")
        
    except Exception as e:
        print(f"Error analyzing data: {str(e)}")
        if 'conn' in locals() and conn:
            conn.close()

def create_visualizations(product_sales, monthly_trend):
    """
    Create and save visualizations
    
    Args:
        product_sales (pandas.DataFrame): Product sales data
        monthly_trend (pandas.DataFrame): Monthly sales trend data
    """
    try:
        # 1. Bar chart of total revenue by product
        plt.figure(figsize=(12, 6))
        plt.bar(product_sales['product_name'], product_sales['total_revenue'])
        plt.xlabel('Product')
        plt.ylabel('Total Revenue ($)')
        plt.title('Total Revenue by Product')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig(f"{REPORTS_DIR}/revenue_by_product.png")
        
        # 2. Line chart of monthly sales trend
        plt.figure(figsize=(12, 6))
        plt.plot(monthly_trend['month'], monthly_trend['monthly_revenue'], marker='o')
        plt.xlabel('Month')
        plt.ylabel('Monthly Revenue ($)')
        plt.title('Monthly Sales Trend')
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig(f"{REPORTS_DIR}/monthly_sales_trend.png")
        
        print("Visualizations created successfully")
        
    except Exception as e:
        print(f"Error creating visualizations: {str(e)}")

if __name__ == "__main__":
    print(f"Querying sales data from {DATABASE_FILE}...")
    run_sales_queries()