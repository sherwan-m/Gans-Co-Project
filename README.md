# Gans-Co-Project
Technologies used: Data engineering, pipeline, python, AWS, API, ...

![Data-pipeline](https://user-images.githubusercontent.com/56586631/183856686-7563d229-0bfd-45a5-82f1-a5417b5f54d2.png)

Gans is a startup developing an e-scooter-sharing system. It aspires to operate in the most populous cities all around the world. In each city, the company will have hundreds of e-scooters parked in the streets and allow users to rent them by the minute.

However, Gans has seen that its operational success depends on something more mundane: having its scooters parked where users need them.

Ideally, scooters get rearranged organically by having certain users moving from point A to point B, and then an even number of users moving from point B to point A. However, some elements create asymmetries. Here are some of them:

1. In hilly cities, users tend to use scooters to go uphill and then walk downhill.
2. In the morning, there is a general movement from residential neighbourhoods towards the city centre.
3. Whenever it starts raining, e-scooter usage decreases drastically.
4. Whenever planes with backpack young tourists land, a lot of scooters are needed close to the airport.

Either way, the company wants to anticipate as much as possible scooter movements. Predictive modelling is certainly on the roadmap, but the first step is to collect more data, transform it and store it appropriately. This is where you come in: your task will be to collect data from external sources that can potentially help Gans predict e-scooter movement. Since data is needed every day, in real-time and accessible by everyone in the company, the challenge is going to be to **assemble and automate a data pipeline in the cloud**.

Tourists are a big share of the Gans’ user base. And a lot of urban tourists travel by plane, with just a backpack. This is why we want to collect data about flights landing to our cities of interest.

We could scrape a website that contains this information, but most likely it would be an arduous task to maintain the code and clean all the information. A structured way to collect data from the internet is by using APIs.
Every day, the company wants to know which flights will arrive and how is the weather at the next day. To automate this, you will have to come up with code that, when executed, generates tomorrow’s date and transforms it into the format that that particular API endpoint requires. and ... 
![](https://user-images.githubusercontent.com/56586631/183844967-df705415-cf58-4ed7-b6c1-9fc77bb28cfe.png)

Creating an automated data pipeline in the cloud is not a trivial task. There will be two major phases of the project, each with its own sub-phases.

## Phase 1: Local pipeline
In this first phase you will run scripts to collect data from the internet and store the data in a database. The scripts will be executed by your computer, in Jupyter notebooks, and the database will also be created in your local MySQL instance —also on your own computer.

### 1.1. Scrape data from the web
Some of the data you will need is going to be floating around the internet, as the content of websites. You will have to learn how to access this information by downloading and extracting the HTML code of these sites, mostly using Python’s most popular web scraping library: beautifulsoup.

### 1.2. Collect data with APIs
The internet is full of data providers. To acquire the specific data you need, you will need to learn how to authenticate yourself and assemble a request with the right parameters. All of this is done through little pieces of software called APIs. Python’s requests library is going to be your main tool to interact with APIs.

### 1.3. Create a database model
When you collect data with Python scripts, you will have data stored as dictionaries or Pandas DataFrames. Python objects are great for local exploration and analysis, but not the best format to make data quickly available to the rest of the company. Relational databases are the solution.

Determining the logical structure of the database is an important first step when a company wants to start storing data in a relational database. Which tables will you need? How will these tables be related to each other? Only after answering these questions (and more), you will create the database.

### 1.4. Store data on a local MySQL instance
Once you’ve created the database model, you will test that the connection between Python and MySQL works by setting up the database locally on your computer and storing the data you collected from the APIs and your web scraper on it.

## Phase 2: Cloud Pipeline
If you use Google Drive or Apple’s iCloud, your files are already on the cloud. The cloud is a catch-all name for any technological resources or services accessed via the internet. And it has many advantages when it comes to building data pipelines: scalability, flexibility, automation, maintenance…

### 2.1. Set up a cloud database
The first step in moving your pipeline to the cloud will be the storage one. You will use RDS, the Relational Database Service from the largest public cloud provider: Amazon Web Services (AWS), to set up your MySQL database.

### 2.2. Move your scripts to Lambda
Lambda is an AWS service for running code seamlessly in the cloud. You will move your data collection scripts from Jupyter Notebooks into AWS Lambda functions.

### 2.3. Automate the pipeline
One of the advantages of running code in AWS is that scheduling and automation are easy. In our case, we will use CloudWatch Events / EventBridge to create rules that will trigger the execution of the data collection scripts.


# Set up a local database
The first step when planning a database is to grab a pen and paper and create a draft that includes:

* All the tables in the database.
* All the columns for each table.
* The relationships for those tables.

### 1.1. Static cities table
We will need a table with city names for all the cities we plan to work with. This table will be filled manually, and it will be static (a new row will only be added whenever the data collection expands to new cities). An important element of this table will be the city_id we will assign to each city. This unique identifier will be the Primary Key of the table, and the identifier for each city in other tables.

Here’s ours :
![cities](https://user-images.githubusercontent.com/56586631/183856742-4b4c233b-4775-443c-a829-9d34603eb7f6.JPG)


Every year we will update the cities data for each city. Instead of overwriting the historical data.

### 1.2. Weathers table
The table where we store the forecast for each city is straightforward: it must identify the city with its city_id and the time of the forecast. Then, you can add as many columns as needed for each one of the weather features you are fetching from the API. Here’s ours:
![weatherJPG](https://user-images.githubusercontent.com/56586631/183856763-2dd67cc7-04f4-481c-a0ee-f6702318c084.JPG)

Every day we will add new weather data in this table.

###  1.3. Static cities-airports table
We would like to build a flights table to store our Aerodatabox data the same way we built the weathers one, but… planes do not land directly in a city: they land at an airport. The tricky part is:
![airports](https://user-images.githubusercontent.com/56586631/183856828-9ef7c22a-49c4-4faa-9764-1f9f5523ce46.JPG)

Each city can have more than one airport (e.g. London has, at least, Gatwick, Heathrow and Standsted).
Each airport can serve more than one city (e.g. the EuroAirport Basel Mulhouse Freiburg serves three cities in Switzerland, France and Germany).
This means we face a many-to-many relationship. Even if, right now, we will not collect data from an international airport serving multiple cities, it’s a good practice to create a future-proof data model. But still, let’s create a database that will be able to hold the mentioned use cases.

This table is also static: it should only change in the rare event that a new airport is built or a new city added. And like cities table, we will update it yearly.

### 1.4. Flights table
We can now build the flights table and be confident that, by capturing the code of the airports, we know the city where are planes landing, thanks to the cities_airpots table.

A little detail here: two airlines can share the same flight. When that’s the case, they should have different flight numbers, but at this point, we are unsure if this condition will always be met. To prevent our table to be filled with duplicates, we will create a column called arrivals_id with a unique identifier within our table (a simple integer, starting at 1), and set it as the PRIMARY KEY of the table:
![arrivalsJPG](https://user-images.githubusercontent.com/56586631/183857026-25f23117-2b45-4a91-9623-ef1b42d48857.JPG)
### 1.5. Transports, Hotels, Landmarks
![transportsJPG](https://user-images.githubusercontent.com/56586631/183857061-06beb557-2733-4b06-9177-8e928412e93e.JPG)


The EER diagram of our database will be : 
![EER](https://user-images.githubusercontent.com/56586631/183857225-d1b1880d-fe90-49ba-b560-d6a16174ecf6.JPG)



# Using SQLAlchemy
SQLAlchemy is the simplest way to connect Python to any SQL. After installing it with pip install SQLAlchemy, you will have to define the details to connect to your database. Then, the method pandas.DataFrame.to_sql() will do the rest for you, converting a DataFrame into a MySQL table in a single step. If the table does not exist yet in your database, it will be created, and you will not even have to worry about data types —they will be inferred from the existing data types in the DataFrame. You can always change the table from MySQLWorkbench using ALTER TABLE together with the MODIFY clause.

Our task now is to adapt the data collection scripts so that every time they are executed, the data they collect gets inserted into your local database.

# code along:
**Phase 1: Local pipeline**

## get_tomorrow():
In this case we want to collect the information of arrival flights and the weather for tomorrow that we would have time to make decisions and apply them. And we made a function to generate tomorrow DateTime format. It returns three outputs:  tomorrow_start,  tomorrow_middle and  tomorrow_end. We will need them for exctract other information.

## get_cities(cities_list,RapidAPI_key):
We use the wikipedia page for each city to extract the wikiid and mayor name. BeautifulSoup is really helpful to do this scraping. And after that we send a requist to "wft-geo-db.p.rapidapi.com" and take the information for the city. Easily we convert the json to pandas dataframe and reshape it how we like. We do this steps for all cities in input list, then we concat the information together and return it.

## weather_forcast(cities_df,openweather_api_key):
This function get the cities dataframe as input and by sending requsts for "api.openweathermap.org" get forecast information for next 5 days in 3 hours timeslides. Then we make a dictionarys for each city and add that part is usefull for us of the json of information. And then we append the dictionaries in a list. We convert that list of dictionarys to pandas dataframe.  
Then we use the tomorrow function and a query to extract just tomorow information.
And it returns the resault as dataframe.

## get_citys_airports(cities_df,RapidAPI_key):
We use the aerodatabox.p.rapidapi.com for each city to extract the airports information. Easily we convert the json to pandas dataframe and reshape it how we like. We do this steps for all cities in cities_df, then we concat the information together and return it. 

## get_arrival_flights(airports,RapidAPI_key):
This function get the airports dataframe as input and by sending requsts for "aerodatabox.p.rapidapi.com/flights/airports/icao/" get next days arrival flights information for each airport. We convert that json to pandas dataframe. We pick the part of data that we want. 
And it returns the resault as dataframe.

## get_citys_informations(cities_df, RapidAPI_key):
This function get the cities dataframe as input and by sending requsts for "hotels4.p.rapidapi.com/" get information about hotels, landmarks and transpoerts for each city. We convert that json to pandas dataframe. We pick the part of data that we want.  
And it returns 3 dataframes: hotels_df, landmark_df and transport_df.

## update_information(cities_list, con):
After that we are able to collect all the data that we want, it is the time to save this in information in our data base.
With the previous functions we are able to collect all the data that we want, it is the time to save this in information in our database. We use sqlalchemy library to generate a connection and then we will push the data in database. But ther is some important things: when and how we want to update our tabels, we explained it at the each table describtion. 

Just a quick reminder:
cities, airports, hotels, transports and landmark : When a new city added to the list
weather, arrivals : every day

At first we read the cities from sql data base, and chek if a new(s) city added to list.
If ther is new city, we will get the information for the cities, airports, hotels, transports and landmark table, we will push the new data in them. 
Then again we read cities and air ports from database and we get the information for tommorrow for the arrivals and weather, and push them in to weather and arrivals tabels.

Here the **Phase 1: Local pipeline** is finished, its works nice, i tried to manage the errors, its not perfect right now but i'll try to make it better.
Its really important to catch all bugs and errors, because we make it automatic. Our code had to have no crush. 


**Phase 2: Cloud Pipeline**

## Transfer all these functions and database to cloud
* Create an account at [Amazon Web Services](https://aws.amazon.com)
* Create a new SQL database in your AWS dashboard
* Copy the contents of aws_lambda_function.py into a lambda function, and replace the placeholders in the SQL connection and the API-Calls with the credentials of your new database
* Create an event-trigger using Amazon Event Bridge to schedule regular execution of the lambda function.

## Contact
* [https://github.com/sherwan-m](https://github.com/sherwan-m)
* [sherwan.mohamadyani@gmail.com](mailto:sherwan.mohamadyani@gmail.com)
