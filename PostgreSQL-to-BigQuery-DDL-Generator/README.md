# PostgreSQL-to-BigQuery DDL Generator

## License
This project is licensed under the MIT License - see the [LICENSE](./LICENSE) file for details.

## Overview
This script automates the generation of BigQuery DDL (Data Definition Language) statements from PostgreSQL table schemas. It supports customizable case transformations and data type mappings to ensure compatibility between PostgreSQL and BigQuery.

## Features
- **Automated DDL Generation**: Converts PostgreSQL table schemas into BigQuery-compatible DDL scripts.
- **Customizable Case Styles**: Supports camel case, upper case, lower case, or no transformation for table and column names.
- **Data Type Mapping**: Handles PostgreSQL-to-BigQuery data type conversions.
- **Multi-table Processing**: Generate DDL scripts for single or multiple tables.

## Prerequisites
- Python 3.x
- PostgreSQL database
- Required Python library: `psycopg2`

Install the library:
```bash
pip install psycopg2
