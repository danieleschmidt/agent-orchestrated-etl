-- Initialize databases for development environment

-- Create ETL application database
CREATE DATABASE etl_db;
CREATE USER etl_user WITH ENCRYPTED PASSWORD 'etl_password';
GRANT ALL PRIVILEGES ON DATABASE etl_db TO etl_user;

-- Create Airflow database
CREATE DATABASE airflow;
CREATE USER airflow WITH ENCRYPTED PASSWORD 'airflow';
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;

-- Create test database
CREATE DATABASE test_etl_db;
CREATE USER test_user WITH ENCRYPTED PASSWORD 'test_password';
GRANT ALL PRIVILEGES ON DATABASE test_etl_db TO test_user;