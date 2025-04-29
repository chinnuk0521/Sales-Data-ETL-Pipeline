<<<<<<< HEAD
# Sales-Data-ETL-Pipeline
=======
# Sales Data ETL Pipeline

This project demonstrates a complete ETL (Extract, Transform, Load) pipeline for processing sales data. The pipeline reads raw sales data from a CSV file, cleans and transforms it, and loads it into a SQLite database for analysis.

## Project Structure

```
sales-etl-pipeline/
│
├── sales_etl.py                  # Main ETL pipeline script
├── generate_sample_data.py       # Script to generate sample data
├── query_data.py                 # Script to query and analyze data
├── requirements.txt              # Project dependencies
├── sales_data.csv                # Sample input data (generated)
├── sales_database.db             # SQLite database (created by ETL)
├── etl_pipeline.log              # Log file (created by ETL)
│
└── reports/                      # Analysis reports and visualizations
    ├── product_sales_summary.csv
    ├── monthly_sales_trend.csv
    ├── revenue_by_product.png
    └── monthly_sales_trend.png
```

## Requirements

- Python 3.8 or higher
- Dependencies listed in `requirements.txt`:
  - pandas
  - numpy
  - matplotlib
  - sqlite3 (included in Python standard library)

## Installation

1. Clone the repository or download the project files.

2. Create a virtual environment (recommended):
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Step 1: Generate Sample Data

Generate a sample CSV file with sales data:

```bash
python generate_sample_data.py
```

This creates `sales_data.csv` with 1000 random sales records, including some missing values to demonstrate data cleaning.

### Step 2: Run the ETL Pipeline

Process the data through the ETL pipeline:

```bash
python sales_etl.py
```

This script:
1. **Extracts** data from `sales_data.csv`
2. **Transforms** the data:
   - Removes rows with missing values
   - Converts dates to standard format
   - Calculates total price
   - Standardizes product names
3. **Loads** the transformed data into a SQLite database (`sales_database.db`)

### Step 3: Query and Analyze the Data

Run analysis on the processed data:

```bash
python query_data.py
```

This script:
1. Queries the database for sales statistics
2. Generates CSV reports in the `reports` directory
3. Creates visualizations (charts and graphs)

## ETL Process Details

### Extract

The Extract step reads raw data from the CSV file:
- Uses pandas' `read_csv()` function
- Validates file existence
- Logs the number of rows extracted

### Transform

The Transform step cleans and enhances the data:
- Removes rows with null values
- Standardizes date format to YYYY-MM-DD
- Calculates total price (quantity × price per unit)
- Standardizes product names to title case
- Logs all transformation steps and statistics

### Load

The Load step inserts data into a SQLite database:
- Creates database and table if they don't exist
- Checks for duplicate records to avoid insertion errors
- Only inserts new records
- Logs the number of records inserted

## Data Analysis

The project includes several analysis queries:
1. Total sales by product
2. Monthly sales trends
3. Top-selling products by quantity
4. Basic database statistics

Visualizations include:
- Bar chart of revenue by product
- Line chart of monthly sales trends

## Customization

- Modify `generate_sample_data.py` to change the characteristics of the sample data
- Add new transformations in the `transform()` function in `sales_etl.py`
- Create additional queries and visualizations in `query_data.py`
- Switch to a different database system by modifying the connection code in `sales_etl.py`

## Logging

The ETL pipeline logs all operations to:
- Console (standard output)
- Log file (`etl_pipeline.log`)

## Project Extensions

Ideas for extending this project:
1. Add incremental loading capability
2. Implement error handling for specific data issues
3. Create a web dashboard to visualize the data
4. Add scheduled execution using cron or Airflow
5. Expand to handle multiple data sources

## License

This project is available under the MIT License.
>>>>>>> 7cf7680 (Remove embedded Git repository)
