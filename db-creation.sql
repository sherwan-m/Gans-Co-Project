DROP DATABASE gans;
CREATE DATABASE IF NOT EXISTS gans; 


describe gans.cities;
use gans;
DROP TABLE IF EXISTS cities;
CREATE TABLE IF NOT EXISTS cities (
	city_id INT,
    wiki_data_id  VARCHAR(50),
    type  VARCHAR(50),
    city VARCHAR(100),
    country VARCHAR(100),
    country_code varchar(50),
    region TEXT,
    mayor TEXT,
    elevation TEXT, 
    latitude TEXT, 
    longitude TEXT, 
    population INT,
    timezone TEXT,
    PRIMARY KEY(city_id)
); 

drop table if exists weather; 
create table if not exists weather (
	weather_id int auto_increment,
    temperature float, 
    temp_min float, 
    temp_max float, 
    feels_like float,
    pressure float,
    humidity float,
    weather TEXT,
    weather_description TEXT,
    clouds float,
    wind_speed float,
    date_time_3h  datetime, 
    city_id INT,
    primary key(weather_id), 
    foreign key (city_id) references cities(city_id)
);

drop table if exists airports;
create table if not exists airports(
	icao varchar(10), 
    iata varchar(10), 
	name text, 
    short_name text,
    latitude_deg float, 
    longitude_deg float, 
    iso_country varchar(10), 
    city_name text, 
    city_id int,
    primary key(icao),
    foreign key (city_id) references cities(city_id)
);

drop table if exists arrivals; 
create table if not exists arrivals(
	arrivals_id int auto_increment, 
    flight_NO varchar(10),
    flight_status text,
    dep_airport text,
    dep_airport_icao varchar(10),
    dep_airport_iata varchar(10),
    sched_arr_loc_time text, 
    sched_arr_Utc text,
    terminal text,
    airline text,
	aircraf text, 
    icao varchar(10),
    primary key (arrivals_id), 
    foreign key (icao) references airports(icao)
);


select * from cities;