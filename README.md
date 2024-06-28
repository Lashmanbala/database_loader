# Database Loader

## Table of Contents
- [Description](#description)
- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration)
- [Supported Formats](#supported-formats)
- [Example](#example)
- [Contact](#contact)

## Description
The Database Loader is a Python application designed to facilitate the loading of data from various sources into a database. This tool supports reading data from CSV files, transforming it as needed, and then loading it into a specified database. It is highly customizable and configurable through environment variables.

## Features
- **Flexible Data Loading**: Supports loading data from CSV files into databases.
- **Environment Variable Configuration**: Easily configure source directories and database connection details.
- **Schema-based Data Processing**: Utilizes a schema to ensure data integrity and correct formatting.
- **Error Handling**: Provides informative messages and handles errors gracefully.

## Installation
### Prerequisites
- Python 3.10
- pip (Python package installer)
- dotenv for managing environment variables

### Steps
1. Clone the repository:
    ```bash
    git clone https://github.com/Lashmanbala/database_loader.git
    cd database_loader
    ```

2. Install the required dependencies:
    ```bash
    pip install -r requirements.txt
    ```

3. Create a `.env` file in the root directory and set the following variables:
    ```env
    SRC_BASE_DIR=/path/to/source/base/dir
    DB_CONNECTION_STRING=your_database_connection_string
    ```

## Usage
To load data into the database, use the following command:
```bash
python app.py '["dataset1", "dataset2"]'
```
Replace ["dataset1", "dataset2"] with the list of datasets you want to load.

## Example
To load the users and transactions datasets:
```bash
python app.py '["categories", "products"]'
```
If you want to process all datasets defined in your schema file, simply run:
```
python app.py
```
## Configuration
The schema for each dataset should be defined in schemas.json located in the source base directory. 
Each dataset's schema should specify the column names and their types.

## Supported Formats
- Input: CSV
- Output: Database table rows

## Contact
For any questions, issues, or suggestions, please feel free to contact the project maintainer:

GitHub: [Lashmanbala](https://github.com/Lashmanbala)

LinkedIn: [Lashmanbala](https://www.linkedin.com/in/lashmanbala/)
