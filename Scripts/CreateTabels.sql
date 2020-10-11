CREATE SCHEMA staging;

CREATE TABLE staging.census (
	latitude float4 NULL,
	longitude float4 NULL,
	blockcode int8 NULL,
	county_x varchar(50) NULL,
	state varchar(100) NULL,
	tract int8 NULL,
	censustract int8 NULL,
	county_y varchar(100) NULL,
	borough varchar(100) NULL,
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
	incomeerr float4 NULL,
	incomepercap float4 NULL,
	incomepercaperr float4 NULL,
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
	census_id int8 NULL,
	census_block_id int8 NULL,
	geom geometry NULL
);


CREATE TABLE staging.grid_250m (
	id int4 NULL,
	wkt text NULL,
	grid_id int8 NULL,
	geom geometry NULL
);


CREATE TABLE staging.taxi_zones (
	borough text NULL,
	"zone" text NULL,
	wkt text NULL,
	locationid int4 NULL,
	service_zone text NULL,
	taxi_zone_id int8 NULL,
	geom geometry NULL
);



CREATE TABLE staging.trips (
	id varchar(25) NULL,
	vendor_id int4 NULL,
	pickup_datetime timestamp NULL,
	dropoff_datetime timestamp NULL,
	passenger_count int4 NULL,
	pickup_longitude float4 NULL,
	pickup_latitude float4 NULL,
	dropoff_longitude float4 NULL,
	dropoff_latitude float4 NULL,
	store_and_fwd_flag varchar(10) NULL,
	trip_duration int4 NULL,
	trip_id int8 NULL,
	pickup_geom geometry NULL,
	dropoff_geom geometry NULL
);