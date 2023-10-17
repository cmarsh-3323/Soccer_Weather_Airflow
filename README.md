# Capstone Project

Project submission for Udacity Data Engineering Nano Degree

Author: Chris Marshall

## Project Summary

 For this project, I wanted to combine weather data with soccer data to see how temperature can effect soccer performance. We can ask questions like 'How does hot/cold weather effect a players aggression/passing/stamina/reactions?' I chose a european soccer database and a temperature database from kaggle that had the data that I was looking for to achieve my analytical goals!

## Getting Started and Usage

* Make sure to have Python installed on your system. You can download python from the official website: [Python Downloads](https://www.python.org/downloads/)

* Create a virtual environment, install the requirements.txt located in this repo and update your airflow home to the working directory that matches the cloned repository.

* To access Airflows web server enter the following command:

    `airflow standalone`

* In airflows UI navigate to admin>>connections>>create and enter in the following values:

    `Conn Id: aws_credentials`
    
    `Conn Type: Amazon Web Services`

    `Login: Access key ID (IAM user credentials)`

    `Password: Secret Access Key (IAM user credentials)`

* Save and create another connection with following values:

    `Conn Id: redshift`

    `Conn Type: Postgres`

    `Host: your redshift clusters endpoint (remove the port at the end!)`

    `Schema: your redshift database (dev is default)`

    `Login: awsuser (default)`

    `Password: your personal password for your redshift cluster`

    `Port: 5439`

* Save connection and now you can run airflow with your redshift cluster! Run the table_setup_dag before running your main DAG

## Data Model
 My decision for this data model was for scalability, performance, and the simplicity of its structure. Star schemas make it easy for data analysts to understand and query the data model. Star schemas work well with popular analytical tools that are analysts can now easily connect to and query data in are model. Our soccer data gets very specific with its statistics and star schemas do a great job handling ad hoc queries that may arise.

 Here is a diagram of the star schema, you can see the fact table in the center connected to multiple dimensional tables.

 ![Data Model](https://github.com/cmarsh-3323/Soccer_Weather_Airflow/blob/main/docs/images/soccer_weather_schema.png?raw=true)

 ## Data Dictionary
 Here is our data dictionary of are final data model after are successful DAG runs:
### Table: Fact_Match

- **ID**: INT (Primary key for the match record)
- **country_id**: INT (Foreign key referencing country_id in Dim_Country)
- **league_id**: INT (Foreign key referencing id in Dim_League)
- **date**: DATE (Date of the match)
- **match_api_id**: INT (Unique identifier for the match)
- **home_team_api_id**: INT (Unique identifier for the home team)
- **away_team_api_id**: INT (Unique identifier for the away team)
- **home_team_goal**: INT (Number of goals scored by the home team)
- **away_team_goal**: INT (Number of goals scored by the away team)
- **home_player_1** through **home_player_11**: FLOAT (Player IDs of home team players)
- **away_player_1** through **away_player_11**: FLOAT (Player IDs of away team players)
- **date_year**: INT (Year of the match)
- **date_month**: INT (Month of the match)
- **date_day**: INT (Day of the match)

### Table: Dim_Weather_Country

- **date**: DATE (Date of the weather data)
- **AverageTemperature**: FLOAT (Average temperature for the specified date and country)
- **AverageTemperatureUncertainty**: FLOAT (Uncertainty in the average temperature)
- **Country**: VARCHAR(255) (Name of the country)
- **date_year**: INT (Year of the weather data)
- **date_month**: INT (Month of the weather data)
- **date_day**: INT (Day of the weather data)

### Table: Dim_Country

- **id**: INT (Primary key for the country record)
- **country_name**: VARCHAR(255) (Name of the country)

### Table: Dim_League

- **id**: INT (Primary key for the league record)
- **country_id**: INT (Foreign key referencing id in Dim_Country)
- **league_name**: VARCHAR(255) (Name of european soccer league)

### Table: Dim_Team

- **id**: INT (Primary key for the team record)
- **team_api_id**: INT (Unique identifier for the team)
- **team_fifa_api_id**: FLOAT (FIFA identifier for the team)
- **team_long_name**: VARCHAR(255) (Full team name)
- **team_short_name**: VARCHAR(255) (Short team name)

### Table: Dim_Team_Attributes

- **id**: INT (Primary key for the team attributes record)
- **team_api_id**: INT (Unique identifier for the team)
- **team_fifa_api_id**: INT (FIFA identifier for the team)
- **date**: DATE (Date of the attributes)
- **buildUpPlaySpeed**: INT (Build-up play speed attribute)
- **buildUpPlaySpeedClass**: VARCHAR(255) (Class of build-up play speed)
- **buildUpPlayDribbling**: FLOAT (Build-up play dribbling attribute)
- **buildUpPlayDribblingClass**: VARCHAR(255) (Class of build-up play dribbling)
- **buildUpPlayPassing**: INT (Build-up play passing attribute)
- **buildUpPlayPassingClass**: VARCHAR(255) (Class of build-up play passing)
- **buildUpPlayPositioningClass**: VARCHAR(255) (Positioning class in build-up play)
- **chanceCreationPassing**: INT (Chance creation passing attribute)
- **chanceCreationPassingClass**: VARCHAR(255) (Class of chance creation passing)
- **chanceCreationCrossing**: INT (Chance creation crossing attribute)
- **chanceCreationCrossingClass**: VARCHAR(255) (Class of chance creation crossing)
- **chanceCreationShooting**: INT (Chance creation shooting attribute)
- **chanceCreationShootingClass**: VARCHAR(255) (Class of chance creation shooting)
- **chanceCreationPositioningClass**: VARCHAR(255) (Positioning class in chance creation)
- **defencePressure**: INT (Defencive pressure attribute)
- **defencePressureClass**: VARCHAR(255) (Class of defencive pressure)
- **defenceAggression**: INT (Defencive aggression attribute)
- **defenceAggressionClass**: VARCHAR(255) (Class of defencive aggression)

### Table: Dim_Player

- **id**: INT (Primary key for the player record)
- **player_api_id**: INT (Unique identifier for the player)
- **player_name**: VARCHAR(255) (Full name of the player)
- **player_fifa_api_id**: INT (FIFA identifier for the player)
- **birthday**: TIMESTAMP (Date of birth of the player)
- **height**: FLOAT (Height of the player in meters)
- **weight**: INT (Weight of the player in kilograms)

### Table: Dim_Player_Attributes

- **id**: INT (Primary key for the player attributes record)
- **player_fifa_api_id**: INT (FIFA identifier for the player)
- **player_api_id**: INT (Unique identifier for the player)
- **date**: DATE (Date of the attributes)
- **overall_rating**: DECIMAL (Overall player rating attribute)
- **potential**: DECIMAL (Players potential attribute)
- **crossing**: DECIMAL (Crossing attribute)
- **finishing**: DECIMAL (Finishing attribute)
- **heading_accuracy**: DECIMAL (Heading accuracy attribute)
- **short_passing**: DECIMAL (Short passing attribute)
- **dribbling**: DECIMAL (Dribbling attribute)
- **free_kick_accuracy**: DECIMAL (Free kick accuracy attribute)
- **ball_control**: DECIMAL (Ball control attribute)
- **sprint_speed**: DECIMAL (Sprint speed attribute)
- **reactions**: DECIMAL (Reactions attribute)
- **stamina**: DECIMAL (Stamina attribute)
- **strength**: DECIMAL (Strength attribute)
- **aggression**: DECIMAL (Aggression attribute)
- **positioning**: DECIMAL (Positioning attribute)
- **penalties**: DECIMAL (Penalties attribute)

 
 ## DAGs

`table setup DAG`

![table_dag](https://github.com/cmarsh-3323/Soccer_Weather_Airflow/blob/main/docs/images/table_dag_success.png?raw=true)

`Chris DAG with task dependencies running successfully`

![chris_dag](https://github.com/cmarsh-3323/Soccer_Weather_Airflow/blob/main/docs/images/graph_view.png?raw=true)
## Operator Architecture

### Preprocess Operator:
   `preprocess_op.py`
* Runs data preprocessing tasks and uploads the processed data to my S3 bucket.
* It first retrieves AWS credentials, begins S3 session, and reads CSV files from a designated S3 bucket.
* The operator then filters and manipulates the data, adding date columns(Year, Month, Day), before uploading the processed DataFrames back to the S3 bucket.
* It also downloads a SQLite database from a specified S3 URL, extracts tables, applies data cleaning operations, and uploads them to the S3 bucket.
* Logs progress and notifies when operation is complete

### Stage Operator:
  `staging_op.py`      
* Initialized by parameters like Redshift connection ID, AWS credentials ID, destination table name, S3 bucket, S3 key, and AWS region
* Builds a COPY command, defining the source location in the S3 bucket and the target table in Redshift
* Retrieves AWS credentials and connects to the Redshift database
* Executes the COPY command using a PostgresHook
* Logs progress and notifies when operation is complete

### Fact Operator:
  `load_fact_op.py`      
* Efficiently loads data into the Fact_Match table.
* Uses our provided SQL statement for the insertion process.
* Inherits parameters for Redshift connection, destination table, SQL query, and schema.
* Logs progress and notifies when the loading operation in the fact table is complete.

### Dimension Operator:
`load_dims_op.py`
* Efficiently loads data into the song, user, artist, and time tables.
* Uses our provided SQL statement for the insertion process.
* Inherits parameters for Redshift connection, destination table, SQL query, and schema.
* Logs progress and notifies when the loading operation in all of the dimension tables are complete.

 ### Data Quality Operator:
`data_quality_check_op.py`       
* Validates data quality by running checks on all tables in our Redshift database.
* Checks for duplicate records running a sql query and raises a value error if the data quality check fails.
* Raises an exception if a table has no results. Making sure tables are not empty.
* I also added SLAs (service level agreements) to all the load operators.

## FAQs

How to approach this project if the data was increased by 100 times?

`I went with a star schema because they are able to handle large amounts of data very efficiently. I would consider increasing the number of nodes when creating the redshift cluster and also choose proper distribution keys for my tables. Schedule partitioning to reduce the amount of data our pipelines need to process. I would also highly consider changing my ETL code for better performance by using bulk inserts or using proper data transformation techniques that can save processing time.`

How can I run my pipeline on a daily basis by 7AM every day?

`To do this we would need to add a schedule_interval parameter within the DAG and schedule it to run at 7 am daily. The parameter would look like this.`
* `schedule_interval='0 7 * * *'`


If my database needed to be accessed by over 100 people, How can I accomplish this?

`I chose Amazon Redshift because it can handle large-scale data warehousing workloads which makes it accessible for over hundreds of people. I would use redshifts WLM(workload management) to assign and prioritize resources to different users. I would also use IAM roles and VPC security groups to make sure only authorized users could access the data. Security is priority when the database is being accessed by more and more people.`

## Tools & Technologies

Apache Airflow: Popular open-source platform that schedules and monitors your workflow. I chose airflow because I enjoy using airflows UI for monitoring and DAG status which allows me to view the logs when a task fails and make assessments to my code.

AWS S3: Used to store and retrieve large amounts of data from anywhere on the web. This was an easy choice for its reliabilty and affordability for my weather and soccer data.

Amazon Redshift: Data warehousing service designed for large-scale analytics. Redshift is perfect for our analytical goals becuase of its fast query execution on large datasets using its MPP(massively parallel processing)

`here are the logos of tools I used to complete this project`

<img align="left" alt="Python" width="26px" src="https://raw.githubusercontent.com/github/explore/80688e429a7d4ef2fca1e82350fe8e3517d3494d/topics/python/python.png" />
<img align="left" alt="Jupyter" width="26px" src="https://raw.githubusercontent.com/github/explore/80688e429a7d4ef2fca1e82350fe8e3517d3494d/topics/jupyter-notebook/jupyter-notebook.png" /> <img align="left" alt="Pandas" width="26px" src="https://pandas.pydata.org/static/img/pandas_secondary.svg" />
<img align="left" alt="SQL" width="26px" src="https://raw.githubusercontent.com/github/explore/80688e429a7d4ef2fca1e82350fe8e3517d3494d/topics/sql/sql.png" /> <img align="left" alt="aws" width="26px" src="https://raw.githubusercontent.com/github/explore/fbceb94436312b6dacde68d122a5b9c7d11f9524/topics/aws/aws.png" />
<img align="left" alt="Visual Studio Code" width="26px" src="https://raw.githubusercontent.com/github/explore/80688e429a7d4ef2fca1e82350fe8e3517d3494d/topics/visual-studio-code/visual-studio-code.png" /><img align="left" alt="Terminal" width="26px" src="https://raw.githubusercontent.com/github/explore/80688e429a7d4ef2fca1e82350fe8e3517d3494d/topics/terminal/terminal.png" />
<img align="left" alt="GitHub" width="26px" src="https://raw.githubusercontent.com/github/explore/78df643247d429f6cc873026c0622819ad797942/topics/github/github.png" />

<br>

## Database Sources

 My dataset choices were both provided by kaggle, a great platform to find open source datasets!

[European Soccer Database](https://www.kaggle.com/datasets/hugomathien/soccer)

* Free open source soccer database perfect for our data analytical needs
* Over 25,000 matches, 10,000 players and 11 European Countries
* The most popular european leagues with detailed match data from 2008 to 2016

[Temperature Database](https://www.kaggle.com/datasets/berkeleyearth/climate-change-earth-surface-temperature-data?select=GlobalLandTemperaturesByCountry.csv)

* Earth Surface Temperature Data
* Our primary focus will be on Europe
* Global Average Land Temperature by City

## Notes

Normally passwords should be redacted and excluded from repositories, however this repo is for proof of concept only and was designed to run on local machines only. If run in a cloud environment all security precautions should be taken.

## Next Steps
If I were working with a team on this project, one of the crucial next steps would be to enhance the dataset by incorporating city, latitude, and longitude information for each of the soccer matches. This additional geographical data would significantly increase the granularity and predictive power of our analysis. Here's why this step is important:

Introducing city-level information provides us with valuable geographic context. This context can help us understand the specific weather conditions in the location where each soccer match is played. Weather can vary significantly from one city to another, even on the same day, and having this data would allow us to account for these local variations.

To harness the full potential of this geographic information, we can tap into the un-leveraged preprocessed city weather data that provides city-level temperature data. By linking our soccer match data with this extensive multi-million row city temperature dataset, we can gain access to highly detailed and localized weather information. This granular data can be used to create more accurate and tailored predictive models.

## References

[Python Standard Library](https://docs.python.org/3/library/index.html)

[Pandas](https://pandas.pydata.org/pandas-docs/stable/user_guide/10min.html)

[AWS Docs](https://docs.aws.amazon.com/)

[Airflow Docs](https://airflow.apache.org/docs/)

[Airflow Macros](https://airflow.apache.org/docs/apache-airflow/1.10.5/macros.html)

[Redshift Cluster Guide](https://docs.aws.amazon.com/redshift/latest/gsg/rs-gsg-launch-sample-cluster.html)

[Redshift Datatypes](https://docs.aws.amazon.com/redshift/latest/dg/c_Supported_data_types.html)

[Redshift Copy Syntax](https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html#r_COPY-syntax)
