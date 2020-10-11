## New York Census

The data from here is based on the boro each point located. This data can be access by group by the table Taxi Zones boro field.

* totalpop - total population
* men - total men population
* women - total women population
* hispanic - prcent of hispanic population
* white - prcent of white population
* black - prcent of black population
* native - prcent of hispanic population
* asian - prcent of asian population
* income - mean income
* poverty - prcent of population in poverty
* childpovert
* professional - prcent of professional sector
* service - prcent of service sector
* office - prcent of office sector
* construction - prcent of construction sector
* production - prcent of production sector
* drive - prcent of population drive to work
* carpool - prcent of population carpool to work
* transit - prcent of population transit to work
* walk - prcent of population walk to work
* workathome - prcent of population who work at home 
* employed - total employed
* privatework - prcent of the population who work at the private sector
* publicwork - prcent of the population who work at the public sector
* selfemployed - prcent of the population who are self employed
* unemployment - prcent of the population who are unemploy 
* census_id - my pk
* geom - geometry field
* grid_id - fk from grid table
* taxi_zone_id - fk from zone table

## New York City Taxi Trip

* trip_id - my pk
* id - an original unique identifier for each trip
* passenger_count - the number of passengers in the vehicle (driver entered value)
* pickup_grid_id - fk from grid table
* pickup_taxi_id - fk from taxi zones
* dropoff_grid_id - fk from grid table
* dropoff_taxi_id - fk from taxi zones
* pickup_geom - geometry field of pickup location
* dropoff_geom - geometry field of dropoff location
* dropoff_latitude - the latitude where the meter was disengaged
* trip_duration - duration of the trip in seconds

## nyc_trips_datetime

the table divide to year, month, day, hour and minute for dropoff and pickup dates and time. pickup datetime is date and time when the meter was engaged, while dropoff is when the meter was disengaged.

* trip_id - fk from trips table


## Taxi Zones

* taxi_zone_id - an original unique identifier for each area
* geom - geometry field
* borough - the name of the borough area
* zone - the nyc zone name area
* wkt - coordinates as text string
* locationid - an original unique identifier for each area
* service_zone - the taxi zone name

## Grid 250m*250m

* grid_id - an original unique identifier for each rectangle
* wkt - coordinates as text string
* geom - geometry field