# Local ETL Pipeline

A local ETL (Extract, Transform, Load) pipeline that ingests CSV data, transforms it with Python/Pandas, converts to Parquet format, and enables querying via DuckDB with Azure Cloud storage integration.

## Overview

This project implements a complete data pipeline with two main components:

1. **CSVDataSource**: Extracts CSV files, transforms them to DataFrames, and loads them as Parquet files to Azure Cloud
2. **DataIngestor**: Downloads Parquet files from Azure Cloud and creates queryable DuckDB tables in-memory

## Architecture

```
CSV Files → Extract → Transform (Pandas) → Parquet → Azure Cloud
                                                           ↓
                                              Download Parquet Files
                                                           ↓
                                              DuckDB In-Memory Tables
```

## Features

- ✅ CSV file ingestion from local directories
- ✅ Data transformation using Pandas DataFrames
- ✅ Parquet format conversion for efficient storage
- ✅ Azure Blob Storage integration
- ✅ DuckDB in-memory database for fast querying
- ✅ Comprehensive logging system
- ✅ Error handling and validation

## Prerequisites

- Python 3.8+
- Azure Storage Account (for cloud integration)
- Required Python packages:
  - pandas
  - pyarrow
  - duckdb
  - azure-storage-blob (assumed from cloud connection)

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd <project-directory>
```

2. Install dependencies:
```bash
pip install pandas pyarrow duckdb azure-storage-blob
```

3. Set up Azure credentials:
   - Configure your Azure connection in `src/cloud/cloud_connection.py`
   - Ensure proper authentication credentials are set

## Project Structure

```
project/
├── src/
│   ├── data/              # Default CSV storage directory
│   │   └── temp_data/     # Temporary parquet downloads
│   ├── data_source/
│   │   └── csv_data_source.py    # CSV extraction and transformation
│   ├── controllers/
│   │   └── controller.py          # Data ingestion and DuckDB tables
│   └── cloud/
│       └── cloud_connection.py    # Azure Cloud integration
├── main.py                # Application entry point
└── README.md
```

## Usage

### Basic Usage

Run the complete ETL pipeline:

```bash
python main.py
```

This will:
1. Extract CSV files from `src/data/`
2. Transform them to Pandas DataFrames
3. Convert to Parquet format
4. Upload to Azure Cloud Storage
5. Download Parquet files back
6. Create DuckDB tables for querying

### CSVDataSource Component

```python
from src.data_source.csv_data_source import CSVDataSource

# Initialize with custom paths
data_source = CSVDataSource(
    default_path='path/to/csv/files',
    download_path='path/to/downloads'
)

# Run the pipeline
data_source.start()
```

**Pipeline Steps:**
1. **Extract**: Scans directory for `.csv` files
2. **Transform**: Reads CSV into Pandas DataFrames
3. **Load**: Converts to Parquet and uploads to Azure

### DataIngestor Component

```python
from src.controllers.controller import DataIngestor

# Initialize the ingestor
ingestor = DataIngestor()

# Run the ingestion pipeline
ingestor.start()
```

**Pipeline Steps:**
1. Downloads Parquet files from Azure Cloud
2. Creates DuckDB in-memory tables
3. Executes queries and displays results

## Data Flow

### 1. CSV Extraction
- Reads all `.csv` files from the specified directory
- Logs each file collected
- Returns list of file paths

### 2. Data Transformation
- Converts CSV files to Pandas DataFrames
- Stores in dictionary: `{filename: DataFrame}`
- Validates data is not empty

### 3. Parquet Conversion & Upload
- Converts DataFrames to Parquet format using PyArrow
- Uploads to Azure Blob Storage under `raw/` prefix
- Files stored as: `raw/{filename}.parquet`

### 4. Cloud Download
- Lists all Parquet files in Azure
- Downloads to local `src/data/temp_data/`
- Maintains file structure

### 5. DuckDB Table Creation
- Registers Parquet files as DuckDB tables
- Tables named after file stems
- Enables SQL querying on the data

## Logging

The pipeline includes comprehensive logging at each step:

```
2024-02-10 10:30:15 | csv_data_source | INFO | Iniciando Pipeline de Dados...
2024-02-10 10:30:15 | csv_data_source | INFO | Inicindo Coleta de Dados...
2024-02-10 10:30:15 | csv_data_source | INFO | Coletando arquivo: data.csv...
2024-02-10 10:30:16 | csv_data_source | INFO | Pipeline concluída com sucesso em 1.23s
```

## Error Handling

The pipeline includes robust error handling:

- **Empty file lists**: Raises `ValueError` with descriptive message
- **Missing data**: Logs warnings and raises exceptions
- **Cloud upload/download errors**: Captures and logs Azure exceptions
- **DuckDB errors**: Handles table creation failures

## Configuration

### Default Paths

- **CSV Source**: `src/data/`
- **Temp Downloads**: `src/data/temp_data/`
- **Cloud Prefix**: `raw/`

### Customization

You can customize paths during initialization:

```python
data_source = CSVDataSource(
    default_path='custom/csv/path',
    download_path='custom/download/path'
)
```

## Example Workflow

1. Place CSV files in `src/data/`:
```
src/data/
├── sales.csv
├── customers.csv
└── products.csv
```

2. Run the pipeline:
```bash
python main.py
```

3. The pipeline will:
   - Convert files to Parquet
   - Upload to Azure as:
     - `raw/sales.parquet`
     - `raw/customers.parquet`
     - `raw/products.parquet`
   - Create DuckDB tables: `sales`, `customers`, `products`

4. Query the data using DuckDB SQL

## Performance

- **Parquet Format**: Columnar storage for efficient compression and faster queries
- **DuckDB**: In-memory analytics database optimized for OLAP workloads
- **Pipeline Timing**: Automatic logging of execution time for monitoring

## Requirements

```txt
pandas>=1.5.0
pyarrow>=10.0.0
duckdb>=0.8.0
azure-storage-blob>=12.0.0
```

## Future Enhancements

- [ ] Support for multiple data sources (JSON, Excel)
- [ ] Incremental data loading
- [ ] Data quality validation rules
- [ ] Scheduling capabilities (Airflow, Prefect)
- [ ] Data lineage tracking
- [ ] SQL query interface for DuckDB
- [ ] Data profiling and statistics

## Troubleshooting

### Issue: "No files found"
- Ensure CSV files are in the correct directory
- Check file permissions
- Verify files have `.csv` extension

### Issue: "Azure connection failed"
- Verify Azure credentials
- Check network connectivity
- Confirm storage account exists

### Issue: "DuckDB table creation failed"
- Ensure Parquet files are valid
- Check available memory
- Verify file paths are correct