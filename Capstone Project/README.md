# Capstone Project - US I-94 Immigration Arrival Data

## 1. Introduction
This project is to build an infrastructure allowing easily querying the data, which resides in a directory of SAS files and CSV files.

Source datasets include:
- **I94 Immigration Data**: This data comes from the US National Tourism and Trade Office in sas7bdat format.
- **U.S. City Demographic Data**: This data comes from OpenSoft in csv format.
- **Airport Code Data**: This is a simple table of airport codes and corresponding cities in csv format.

We are trying to create a streamlined ETL to enable convenient analysis. The ETL pipeline extracts data, processes them using Spark, and loads the data back as a set of dimensional tables stored in Spark parquet files. This will allow one to easily find insights in I-94 arrival data.

## 2. Database Tables
### Database Schema
Star Schema is utilized to construct database, consisting of one fact table and 6 dimentional tables.

#### Fact Table
1. **arrivals** - records of arrival recorded under I-94 program.
    - i94yr, i94mon, i94cit, i94res, i94port, arrdate, i94mode, i94addr, depdate, i94bir, i94visa, count, dtadfile, visapost, occup, entdepa, entdepd, entdepu, matflag, dtaddto, insnum, airline, admnum, fltno, visatype

#### Dimension Tables
2. **admissions** - visitors who are given admission numbers
    - admnum, biryear, gender

3. **i94port** - valid US port codes for processing
    - i94port, i94port_name

4. **i94visa** - valid visa codes for processing
    - i94visa, i94visa_name

5. **i94addr** - valid US state for address
    - start_time, hour, day, week, month, year, weekday

6. **i94mode** - modes of arrival
    - i94mode, i94mode_name

7. **i94cit_i94res** - cities or regions valid for processing
    - i94cit_i94res, i94cit_i94res_name

8. **airports** - valid airport codes and corresponding information
    - ident, type, name, elevation_ft, continent, iso_country, iso_region, municipality, gps_code, iata_code, local_code, coordinates

9. **demographics** - US city demographics informaiton
    - city, state, median_age, male_population, female_population, total_population, number_of_veterans, number_of_foreign_born, average_household_size, state_code, race, count

## 3. Files structure
### Markdown
- **README.md** introduction to this project

### Python scripts
- **etl.py** processes SAS files and CSV files and loads the records into tables. 


