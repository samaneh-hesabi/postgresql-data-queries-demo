<div style="font-size:2.5em; font-weight:bold; text-align:center; margin-top:20px;">Data Warehouse Project</div>

This project implements a data warehouse solution using PostgreSQL and Python. It focuses on ETL (Extract, Transform, Load) processes, data modeling, and analytics.

# 1. Project Structure

- `data/`: Directory for storing datasets and data files
  - `raw/`: Contains original, unprocessed data files (CSV format)
  - `processed/`: (Reserved for future use) Contains cleaned and transformed data ready for loading
- `utils/`: Utility modules
  - `logging_utils.py`: Logging configuration
  - `db_utils.py`: Database connection and query utilities
  - `etl_utils.py`: ETL process utilities
  - `data_generator.py`: Script to generate synthetic data
  - `db_setup.py`: Database schema setup script
- `queries/`: SQL query modules
  - `analytics_queries.py`: Sample analytics queries for the sales data warehouse
- `tests/`: Test files
- `requirements.txt`: Project dependencies
- `.gitignore`: Git ignore rules
- `LICENSE`: Project license
- `setup_database.py`: Script to set up the database schema
- `generate_data.py`: Script to generate sample data
- `run_etl.py`: Script to run the ETL process

# 1.1 Prerequisites

Before running the project, ensure you have:

1. PostgreSQL installed and running
2. Python 3.8+ installed
3. Required Python packages:
   ```bash
   pip install -r requirements.txt
   ```

# 1.2 Configuration

Create a `.env` file in the project root with the following variables:

```env
DB_NAME=sales_warehouse
DB_USER=postgres
DB_PASSWORD=your_password
DB_HOST=localhost
DB_PORT=5432
LOG_LEVEL=INFO
```

# 1.3 Features

The project implements:

1. Data warehouse schema design
2. ETL processes
3. Data modeling
4. Analytics queries
5. Data quality checks
6. Performance optimization
7. Monitoring and logging

# 1.4 Project Best Practices

This project follows several best practices:

1. **Environment Variables**: Sensitive information is stored in environment variables
2. **Logging**: Comprehensive logging system for debugging and monitoring
3. **Error Handling**: Proper error handling and reporting
4. **Type Hints**: Python type hints for better code documentation
5. **Database Utilities**: Reusable database connection and query utilities
6. **Testing**: Unit tests for core functionality
7. **Documentation**: Clear documentation and comments
8. **Modular Code**: Code is organized into modules for better maintainability
9. **Version Control**: Git is used for version control with clear commit messages
10. **Code Style**: Code follows PEP 8 style guidelines

# 1.5 Development Workflow

1. Create feature branches for new functionality
2. Write tests for new features
3. Implement features
4. Run tests
5. Create pull requests
6. Code review
7. Merge to main branch

# 1.6 Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a pull request

# 1.7 Data Analysis Results

The project includes sample analytics queries to analyze the sales data. The results of these queries can be found in the `queries/analytics_queries.py` file. To run the analytics queries, use the following command:

```bash
PYTHONPATH=$PYTHONPATH:. python queries/analytics_queries.py
```

This will display the results of the analytics queries in a clean, readable format.
