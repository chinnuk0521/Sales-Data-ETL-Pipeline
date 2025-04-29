#!/usr/bin/env python3
"""
Complete Sales ETL Pipeline Runner

This script runs the complete ETL pipeline from data generation to analysis:
1. Generate sample data
2. Run ETL process (Extract, Transform, Load)
3. Query and analyze the results

Author: Claude
Date: April 29, 2025
"""

import os
import subprocess
import time
import sys

def run_script(script_name, description):
    """
    Run a Python script and capture its output
    
    Args:
        script_name (str): Name of the script to run
        description (str): Description of the script's purpose
    """
    print(f"\n{'='*80}")
    print(f"STEP: {description}")
    print(f"{'='*80}")
    
    # Check if the script exists
    if not os.path.exists(script_name):
        print(f"Error: Script {script_name} not found!")
        sys.exit(1)
    
    # Run the script and capture output
    try:
        start_time = time.time()
        
        # Run the script as a subprocess and capture its output
        result = subprocess.run(
            [sys.executable, script_name], 
            capture_output=True, 
            text=True, 
            check=True
        )
        
        # Print the script's output
        print(result.stdout)
        
        if result.stderr:
            print("ERRORS/WARNINGS:")
            print(result.stderr)
        
        end_time = time.time()
        print(f"Completed in {end_time - start_time:.2f} seconds")
        
    except subprocess.CalledProcessError as e:
        print(f"Error running {script_name}:")
        print(e.stderr)
        if input("Continue with next step? (y/n): ").lower() != 'y':
            sys.exit(1)

def main():
    """
    Run the complete ETL pipeline
    """
    print("\nSALES DATA ETL PIPELINE")
    print("======================\n")
    print("This script will run the complete ETL pipeline from data generation to analysis.")
    
    # List of scripts to run in order
    pipeline_steps = [
        ("generate_sample_data.py", "Generate sample sales data"),
        ("sales_etl.py", "Run ETL process (Extract, Transform, Load)"),
        ("query_data.py", "Query and analyze the results")
    ]
    
    # Run each script in sequence
    for script_name, description in pipeline_steps:
        run_script(script_name, description)
    
    print("\n" + "="*80)
    print("PIPELINE COMPLETE!")
    print("="*80)
    print("\nAll steps of the ETL pipeline have been completed successfully.")
    print("\nResults:")
    print("  - Raw data: sales_data.csv")
    print("  - Processed database: sales_database.db")
    print("  - Analysis reports: ./reports/ directory")
    print("  - Logs: etl_pipeline.log")

if __name__ == "__main__":
    main()