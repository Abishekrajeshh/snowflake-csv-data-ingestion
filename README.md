# Data Conversion and Database Connections with Python

This repository contains Python scripts for converting JSON files to CSV, uploading CSV files to Snowflake, and handling data integration tasks. It includes functions for bulk uploading data, creating and managing Snowflake tables and stages, and handling errors during file operations.

## Repository Contents

1. **JSON to CSV Conversion**: Scripts to convert JSON files to CSV format.
2. **CSV Upload to Snowflake**: Scripts to upload CSV files to Snowflake, including handling table creation, stage management, and data loading.
3. **Bulk Uploading**: Scripts to handle bulk uploading of CSV files from a specified directory to Snowflake with error handling and retry mechanisms.

## Installation

To use the scripts in this repository, you need to install the following Python packages:

- `pandas`: For data manipulation and reading CSV files.
- `snowflake-connector-python`: For connecting and interacting with Snowflake.
- `numpy`: For handling missing values.

You can install these packages using pip:

```bash
pip install pandas snowflake-connector-python numpy
```

## Usage

1. **JSON to CSV Conversion**:
Use the provided scripts to convert JSON files to CSV. Update the script to specify the input JSON file path and output CSV file path.  

2. **CSV Upload to Snowflake:**
Configure the Snowflake connection details in the script.  
Run the script to upload a CSV file to Snowflake, creating or replacing tables and stages as needed.  

3. **Bulk Uploading:**
Update the script to specify the directory path containing CSV files, the Snowflake table name, stage name, and mode (append or overwrite).  
The script will handle multiple CSV files and retry uploading in case of errors.  

## Examples

1. **Convert JSON to CSV:**

```bash
python json_to_csv.py
```  

2. **Upload CSV to Snowflake:**

```bash
python upload_csv_to_snowflake.py
```  

3. **Bulk Upload CSV Files:**

```bash
python bulk_upload_to_snowflake.py
```

## Configuration  

Before running the scripts, ensure you update the following in the Snowflake-related scripts:  
Snowflake connection details (user, password, account, warehouse, database, schema).
File paths and table names as required.

## Troubleshooting  
File Not Found: Ensure the file paths provided are correct and files exist at those locations.  
Snowflake Errors: Check connection details and ensure Snowflake account and permissions are properly set up.  

## Contribution  
Feel free to fork the repository, create a branch, and submit a pull request with improvements or fixes.  

## License  
This repository is licensed under the MIT License. See the LICENSE file for more details.  


