
# data-warehousing
## Summary
The sparkify saga continues:
> A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

In this project, I build an ETL pipeline that extracts their data from S3, stages it in Redshift tables, and extracts, transforms, and loads staging data into a set of fact and dimensional tables.

## Explanation of files
| filename | description  |
|--|--|
`notebooks/Analytics queries.ipynb`|A notebook with analytics queries that can be run after the data is loaded
`utils/create_cluster.py`| A utility for provisioning a Redshift cluster
`.gitignore`| The files which should not be committed; note `dwh.cfg` is listed because credentials should be kept secret
`create_tables.py`	| A script that creates the tables defined in `sql_queries.py`
`etl.py`| A script that executes the load table statements from `sql_queries.py`
`requirements.txt` | Requirements for running this project locally
`sql_queries.py`| SQL statements for table creation and loading

## Purpose of the database

The purpose of this database is to create a logical structure for Sparkify to store transformed logs containing song and user data. These tables provide a structure that enable Sparkify's analysts to query this data.

The `sparkify` database consists of the following tables:

|Table name|Purpose|
|-|-|
|`songs`|Details about each song found in the song data|
|`artists`|Details about each song found in the song data|
|`users`|Details about each song found in the log data|
|`time`|A collection of date parts from each songplay start time|
|`songplays`|Details about each song played, including both song, artist, and user data|
|`songs_stage`|Staging table used during ETL to read in song data from JSON|
|`events_stage`|Staging table used during ETL to read in log data from JSON|

## Schema explanation
Below are an explanation of the database and a walkthrough of how the ETL works.

### Schema design
This section explains the table structures and how they relate to one another. Below is a list of each table along with the following:

1. Table name and description
2. Columns and data types for each table
3. Column constraints, if any
4. An explanation of the data in each column
5. An example query from each table

#### Songs table

Table name: `songs`

Description: A collection of unique songs found across all the `song_data` JSON logs.

##### Table structure
|Column|Type|Modifiers|
|-|-|-|-|
| song_id   | character varying(256) |  not null   |
| title     | character varying(256) |             |
| artist_id | character varying(256) |  not null   |
| year      | integer                |             |
| duration  | double precision       |             |

##### Example
| song_id            | title           | artist_id          | year   | duration   |
|-|-|-|-|-|
| SONHOTT12A8C13493C | Something Girls | AR7G5I41187FB4CE6C | 1982   | 233.40363  |
| SOIAZJW12AB01853F1 | Pink World      | AR8ZCNI1187B9A069B | 1984   | 269.81832  |
| SOFSOCN12A8C143F5D | Face the Ashes  | ARXR32B1187FB57099 | 2007   | 209.60608  |

#### Artists table

Table name: `artists`

Description: A collection of each unique artist found across the `song_data` JSON, including their location, if available.

##### Table structure
| Column           | Type              | Modifiers   | Description |
|-|-|-|-|
| artist_id | character varying(256) |  not null   |
| name      | character varying(256) |  not null   |
| location  | character varying(256) |             |
| latitude  | double precision       |             |
| longitude | double precision       |             |

##### Example
| artist_id          | artist_name                  | artist_location         | artist_latitude   | artist_longitude   |
|-|-|-|-|-|
| ARLTWXK1187FB5A3F8 | King Curtis     | Fort Worth, TX    | 32.74863          | -97.32925          |
| AR3JMC51187B9AE49D | Backstreet Boys | Orlando, FL       | 28.53823          | -81.37739          |
| ARAJPHH1187FB5566A | The Shangri-Las | Queens, NY        | 40.7038           | -73.83168          |
#### Users table

Table name: `users`

Description: Data about each user found in the  `log_data` JSON, including gender and pricing tier.

##### Table structure
| Column     | Type              | Modifiers   | Description|
|-|-|-|-
| user_id    | integer                |  not null   |
| first_name | character varying(256) |  not null   |
| last_name  | character varying(256) |  not null   |
| gender     | character varying(256) |             |
| level      | character varying(256) |             |
##### Example
| user_id   | first_name   | last_name   | gender   | level   |
|-|-|-|-|-|
| 2         | Jizelle      | Benjamin    | F        | free    |
| 3         | Isaac        | Valdez      | M        | free    |
| 4         | Alivia       | Terrell     | F        | free    |

#### Time table
Table name: `time`

Description: timestamps of user songplays broken down into numeric values of the various date part units from that timestamp

##### Table structure
| Column     | Type                        | Modifiers   | Description
|-|-|-|-|
| start_time | timestamp without time zone |  not null   |
| hour       | integer                     |             |
| day        | integer                     |             |
| week       | integer                     |             |
| month      | integer                     |             |
| year       | integer                     |             |
| weekday    | integer                     |             | weekday

##### Example
| start_time          | hour   | day   | week   | month   | year   | weekday   |
|-|-|-|-|-|-|-|
| 2018-11-15 18:04:48 | 18     | 15    | 46     | 11      | 2018   | 4         |
| 2018-11-16 04:06:55 | 4      | 16    | 46     | 11      | 2018   | 5         |

#### Songplays table
Table name: `songplays`

Description: Song and user data for each individual songplay

##### Table structure
|Column|Type|Modifiers|description|
|-|-|-|-|
| songplay_id | integer                     |  default "identity"(100511, 0, '0,1'::text) |
| start_time  | timestamp without time zone |                                             |
| user_id     | integer                     |                                             |
| level       | character varying(256)      |                                             |
| song_id     | character varying(256)      |                                             |
| artist_id   | character varying(256)      |                                             |
| session_id  | integer                     |                                             |
| location    | character varying(256)      |                                             |
| user_agent  | character varying(256)      |                                             |
##### Example

| songplay_id   | start_time          | user_id   | level   | song_id   | artist_id   | session_id   | location                            | user_agent                                                                                                                                  |
|-|-|-|-|-|-|-|-|-|
| 0             | 2018-11-05 01:54:02 | 44        | paid    | <null>             | <null>             | 237          | Waterloo-Cedar Falls, IA                    | Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:31.0) Gecko/20100101 Firefox/31.0                                                           |
| 1             | 2018-11-05 02:01:51 | 44        | paid    | <null>             | <null>             | 237          | Waterloo-Cedar Falls, IA                    | Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:31.0) Gecko/20100101 Firefox/31.0                                                           |
| 2             | 2018-11-05 01:48:00 | 44        | paid    | SOOXLKF12A6D4F594A | ARF5M7Q1187FB501E8 | 237          | Waterloo-Cedar Falls, IA                    | Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:31.0) Gecko/20100101 Firefox/31.0                                                           |

### ETL walkthrough
* `create_tables.py` creates the analytics tables from the above section along with two staging tables used for ETL: `songs_stage` and `events_stage`. 
* The `etl.py` script executes a series of SQL statements that can be found in `sql_queries.py` that:
	* copy the json from S3 into the staging tables
	* perform transformations or filtering as necessary 
	* load the desired values into the proper target tables

## Requirements
This project was written in Python3 using `psycopg2`. The Redshift cluster was provisioned with `utils/create_cluster.py` using a `dwh.cfg` containing AWS credentials and `boto3`. That said it requires either a cluster or AWS credentials and permission to create a cluster. See Getting Started for more details.

The python requirements are listed in `requirements.txt`. It's best to install these packages in a virtual [environment](%28https://packaging.python.org/guides/installing-using-pip-and-virtual-environments/#targetText=To%20create%20a%20virtual%20environment,project%27s%20directory%20and%20run%20virtualenv.&targetText=The%20second%20argument%20is%20the,installation%20in%20the%20env%20folder.%29).

The Python libraries used can all be installed with pip
* `psycopg2` to connect to the database and execute queries
`jupyter` and `pandas`  to run the sample query notebook locally
* [Optional] [`boto3`]([https://github.com/boto/boto3](https://github.com/boto/boto3)) to use `utils/create_cluster.py` to provision the Redshift cluster

This should get you there:
`git@github.com:ScottFitzgerald83/data-warehousing.git`
`cd data-warehousing`
`python3 -m venv venv`
`pip install -r requirements.txt`


## Getting started

### Setting up the cluster
Feel free to skip this step if you have an existing cluster or don't need help setting one up. In `utils/create_cluster` there is a cluster provisioning script that reads from a file `/dwh.cfg` in the root level of the project directory. The `dwh.cfg` should resemble the following structure:
```
[AWS]
KEY=
SECRET=
REGION_NAME=

[DWH]
DWH_CLUSTER_TYPE=multi-node
DWH_NUM_NODES=2
DWH_NODE_TYPE=dc2.large

DWH_IAM_ROLE_NAME=
DWH_CLUSTER_IDENTIFIER=
DWH_DB=
DWH_DB_USER=
DWH_DB_PASSWORD=
DWH_PORT=

[CLUSTER]
HOST=
DB_NAME=
DB_USER=
DB_PASSWORD=
DB_PORT=

[IAM_ROLE]
ARN=

[S3]
LOG_DATA='s3://udacity-dend/log_data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song_data'
```
### Running the ETL
1. Run `create_tables` to create the Redshift tables: `python3 create_tables.py`
2. Run the ETL pipeline to load the tables: `python3 etl.py`

### Running sample queries
There is a sample notebook in this project located at `/notebooks/sample_queries.ipynb`. Once the Redshift cluster is up and the tables have been created and ETLed, you can run the queries therein against the `songplays` table or create your own. The provided queries are as follows: 
1. Most popular songs
2. Locations which listened to the most songs
3. Top user agents
4. Top users
5. Average number of songs per session
6. Count of songplays grouped by pricing tier (level)
