# ✈️ BigData Airlines

This project focuses on processing and analyzing Brazilian air traffic data using public datasets and APIs. The steps include data normalization, enrichment via an external API, and creation of insightful views using SQL.

---

## Project Steps

### 1. Load VRA Data
- Normalize column headers to `snake_case`
- Save the processed data

### 2. Load AIR_CIA Data
- Normalize column headers to `snake_case`
- Split the `ICAO IATA` column into two:
  - The values are separated by a space
  - The IATA code may be missing; if so, set it to `null`
- Save the processed data

### 3. Create `aerodromes` Table
- Use the [Airport Info API](https://rapidapi.com/Active-api/api/airport-info/) to fetch aerodrome information using ICAO codes from the VRA dataset
- Save the enriched data into a new table called `aerodromes`

---

## SQL Views

### View 1: Most Used Route per Airline
For each airline, identify the most frequently used route with the following information:
- Corporate name of the airline
- Name of the origin airport
- ICAO code of the origin airport
- State (UF) of the origin airport
- Name of the destination airport
- ICAO code of the destination airport
- State (UF) of the destination airport

### View 2: Most Active Airline per Airport
For each airport, find the airline with the most activity over the year, including:
- Name of the airport
- ICAO code of the airport
- Corporate name of the airline
- Number of routes **departing** from the airport
- Number of routes **arriving** at the airport
- Total number of **landings and takeoffs**

---

## Data Sources
- **VRA (Flight Data)**  
- **AIR_CIA (Airline Company Information)**  
- **[Airport Info API](https://rapidapi.com/Active-api/api/airport-info/)**

---

## Notes
- Ensure proper handling of missing or inconsistent values
- Normalize datasets before creating views
- Use efficient SQL queries to optimize performance on large datasets

