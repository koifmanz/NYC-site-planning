# Udacity Final Project - NYC ETL

Udacity's capstone project purpose is to give combine what I learned throughout the program. In this project, I could choose to complete the project provided for me from Udacity, or define the scope and data for a project of your own design. I decided the latter after I was inspired by post in [Carto's blog](https://carto.com/blog/site-planning-coverage-optimization-mobility-data/).

Carto's prestend a study case from Spain, which try to achive two goals:
1.  Identifying the optimal locations for new markets.
2.  Calculating the catchment areas for locations for which we are lacking historical customer data.

For my project I used datasets of New-York city, which available on the web. A full discussion about the project's goals, steps and tools can be found in *writeup.md*. The data can be download from 7z file here in github.

---

## The datasets

I used 4 datasets:

1. [NYC Census data](https://www.kaggle.com/muonneutrino/new-york-city-census-data) - 2 csv files, around 40k rows.
2. [NYC Yellow cab trips data](https://www.kaggle.com/c/nyc-taxi-trip-duration/) - csv file, around 1.4m rows.
3. [NYC Taxi Zones](https://data.cityofnewyork.us/Transportation/NYC-Taxi-Zones/d3c5-ddgc) - I download the data as shapefile and convert it manully to csv with some manpulation. around 250 rows.
4. NYC 250*250 meter grid - json file, manully made grid based on Carto's blog post. 

The final database fields details can be found in *dict.md*


---

### How to run the script

1. Edit *config.cfg* with your postgresql connection info and the files locations.
2. run both *CreateTables.sql* and *CreateFinalTables.sql*. The first create the schema and the tables for the staging part, while the latter create the schema and tables for the loading part.
3. run *etl.py* (or from the notebook with same name).

I upload my *requirements.txt* from Anaconda, but here the most important libraries:
* numpy
* psycopg2
* pandas
* PostGIS - Postgresql spatial database extender. 


---

## To Do

1. Move to AWS
2. Add more datasets (Place of interest, etc.)
3. 
