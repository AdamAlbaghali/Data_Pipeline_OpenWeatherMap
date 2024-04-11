**Weather Data Processing DAG**

This Airflow DAG (Directed Acyclic Graph) automates the process of fetching, transforming, and loading weather data for Portland. It retrieves weather information from an API, transforms it into a structured format, and then uploads it to an S3 bucket.

### Prerequisites
- Python 3.x
- Apache Airflow installed
- AWS S3 bucket for storing the processed data
- Access to a Weather Map API with an API key

### Setup
1. Ensure Airflow is properly configured with necessary connections, including `weathermap_api`.
2. Replace placeholder values for AWS credentials (`aws_credentials`) and Weather Map API endpoint with your actual credentials and endpoint.

### DAG Overview
- **DAG Name:** weather_dag
- **Schedule Interval:** Daily (`@daily`)
- **Start Date:** April 11, 2024
- **Owner:** Airflow
- **Email:** myemail@domain.com (for failure notifications)

### Tasks
1. **is_weather_api_ready**
   - **Task ID:** is_weather_api_ready
   - **Description:** Checks if the Weather Map API is ready for data retrieval.
   - **Dependencies:** None
   - **Sensor Type:** HTTP Sensor

2. **extract_weather_data**
   - **Task ID:** extract_weather_data
   - **Description:** Extracts weather data from the Weather Map API for Portland.
   - **Dependencies:** is_weather_api_ready
   - **Operator Type:** Simple HTTP Operator

3. **transform_load_weather_data**
   - **Task ID:** transform_load_weather_data
   - **Description:** Transforms extracted weather data into a structured format and loads it into an S3 bucket.
   - **Dependencies:** extract_weather_data
   - **Operator Type:** Python Operator

### Custom Python Functionality
- **kelvin_to_fahrenheit:** Converts temperature from Kelvin to Fahrenheit.
- **transform_load_data:** Transforms raw weather data into a structured DataFrame and uploads it to an S3 bucket.

### Notes
- The DAG is set to run daily, ensuring up-to-date weather information.
- Adjust the `retries` and `retry_delay` parameters in `default_args` to fine-tune error handling and retry behavior.
- Ensure proper handling of sensitive information such as API keys and credentials.
