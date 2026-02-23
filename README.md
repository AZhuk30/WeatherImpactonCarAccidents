# ADS 507 - Weather Impact on Car Accidents: Real-Time Weather Impact Assessment on NYC Traffic Safety

**Partner(s)/Contributor(s)**:<br>
Alexander Zhuk, Gagandeep Singh, Stephanie Smith

### Project Overview
This project builds an automated ETL pipeline that integrates New York City vehicle collision data with hourly weather data to analyze the impact of weather conditions on traffic safety.
The pipeline is automatically updated daily at 2:00 AM EST using GitHub Actions, ensuring that the system continuously ingests the latest available data.

The system performs the following steps:
1. Extracts vehicle collision data from NYC Open Data
2. Extracts hourly weather data from the Open-Meteo API
3. Transforms and cleans the datasets using Python and SQL
4. Loads the cleaned data into structured master CSV files and MySQL tables
5. Automatically updates datasets daily using GitHub Actions (.github/workflows/update-data.yml)
6. Displays insights using an interactive Streamlit dashboard
7. The pipeline allows users to analyze relationships between weather conditions and vehicle collision frequency and severity.

### Access the Live Dashboard here: https://carsinnyc.streamlit.app/ 
### Access the latest processed data here: https://github.com/AZhuk30/WeatherImpactonCarAccidents/tree/main/data/processed



### To Run Locally:
 Clone the repository:
   ```sh
   git clone https://github.com/AZhuk30/WeatherImpactonCarAccidents.git
   ```
 Install dependencies:
   ```sh
   pip install -r requirements.txt
   ```
 Build MySQL Database:
   ```sh
   sql/schema.sql
   ```
 Run pipeline:
   ```sh
   python run_pipeline.py
   ```
 Run dashboard:
   ```sh
  streamlit run dashboard/app.py
   ```

### Pipeline components
src/extract.py - API data extraction  <br>
src/transform.py - Data cleaning and transformation   <br>
src/load.py - Load data into MySQL and master CSV files   <br>
run_pipeline.py - Main ETL pipeline controller   <br>
sql/schema.sql - MySQL database schema definition   <br>
dashboard/app.py - Streamlit dashboard   <br>


### Data Sources
NYC Open Data Motor Vehicle Collisions - https://data.cityofnewyork.us/resource/h9gi-nx95.json<br>
Open_Meteo Weather API - https://open-meteo.com/
