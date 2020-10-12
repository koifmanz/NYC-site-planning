CREATE SCHEMA "data";

CREATE table IF NOT EXISTS data.nyc_taxi_zones (
	borough text NULL,
	"zone" text NULL,
	wkt text NULL,
	locationid int4 NULL,
	service_zone text NULL,
	taxi_zone_id int8 PRIMARY KEY,
	geom geometry not NULL
);

CREATE table IF NOT EXISTS data.nyc_grid_250m (
	id int4 NULL,
	wkt text null,
	grid_id int8 PRIMARY KEY,
	geom geometry not NULL
);

CREATE table IF NOT EXISTS data.nyc_census (
	totalpop int4 NULL,
	men int4 NULL,
	women int4 NULL,
	hispanic float4 NULL,
	white float4 NULL,
	black float4 NULL,
	native float4 NULL,
	asian float4 NULL,
	citizen int4 NULL,
	income float4 NULL,
	poverty float4 NULL,
	childpoverty float4 NULL,
	professional float4 NULL,
	service float4 NULL,
	office float4 NULL,
	construction float4 NULL,
	production float4 NULL,
	drive float4 NULL,
	carpool float4 NULL,
	transit float4 NULL,
	walk float4 NULL,
	othertransp float4 NULL,
	workathome float4 NULL,
	meancommute float4 NULL,
	employed int4 NULL,
	privatework float4 NULL,
	publicwork float4 NULL,
	selfemployed float4 NULL,
	familywork float4 NULL,
	unemployment float4 NULL,
	census_id int8 PRIMARY KEY,
	geom geometry not NULL,
	grid_id int8 not null REFERENCES nyc_grid_250m(grid_id),
	taxi_zone_id int8 not null REFERENCES nyc_taxi_zones(taxi_zone_id)
);


CREATE table IF NOT EXISTS data.nyc_taxi_trips (
	passenger_count int4 null,
	trip_duration int4 NULL,
	trip_id int8 PRIMARY KEY,
	pickup_geom geometry NOT NULL,
	dropoff_geom geometry not NULL,
	pickup_grid_id int8 not null REFERENCES nyc_grid_250m(grid_id),
	pickup_taxi_zone_id int8  not null REFERENCES nyc_taxi_zones(taxi_zone_id),
	dropoff_grid_id int8 not null REFERENCES nyc_grid_250m(grid_id),
	dropoff_taxi_zone_id int8 not null REFERENCES nyc_taxi_zones(taxi_zone_id)
);

CREATE table IF NOT EXISTS data.nyc_trips_datetime (
	trip_id int8 PRIMARY key references nyc_taxi_trips (trip_id),
	pickup_year float8 NULL,
	pickup_month float8 NULL,
	pickup_day float8 NULL,
	pickup_hour float8 NULL,
	pickup_minute float8 NULL,
	dropoff_year float8 NULL,
	dropoff_month float8 NULL,
	dropoff_day float8 NULL,
	dropoff_hour float8 NULL,
	dropoff_minute float8 NULL
);


