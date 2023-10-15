CREATE TABLE public.Fact_Match (
    ID INT PRIMARY KEY,
    country_id INT,
    league_id INT,
    "date" DATE,
    match_api_id INT,
    home_team_api_id INT,
    away_team_api_id INT,
    home_team_goal INT,
    away_team_goal INT,
    home_player_1 FLOAT,
    home_player_2 FLOAT,
    home_player_3 FLOAT,
    home_player_4 FLOAT,
    home_player_5 FLOAT,
    home_player_6 FLOAT,
    home_player_7 FLOAT,
    home_player_8 FLOAT,
    home_player_9 FLOAT,
    home_player_10 FLOAT,
    home_player_11 FLOAT,
    away_player_1 FLOAT,
    away_player_2 FLOAT,
    away_player_3 FLOAT,
    away_player_4 FLOAT,
    away_player_5 FLOAT,
    away_player_6 FLOAT,
    away_player_7 FLOAT,
    away_player_8 FLOAT,
    away_player_9 FLOAT,
    away_player_10 FLOAT,
    away_player_11 FLOAT,
    date_year INT,
    date_month INT,
    date_day INT
);

CREATE TABLE public.Dim_Weather_Country (
    "date" DATE,
    AverageTemperature FLOAT,
    AverageTemperatureUncertainty FLOAT,
    Country VARCHAR(255),
    date_year INT,
    date_month INT,
    date_day INT,
    PRIMARY KEY (date, Country)
);

CREATE TABLE public.Dim_Country (
    id INT PRIMARY KEY,
    country_name VARCHAR(255) 
);

CREATE TABLE public.Dim_League (
    id INT PRIMARY KEY,
    country_id INT,
    league_name VARCHAR(255),
    FOREIGN KEY (country_id) REFERENCES Dim_Country(id)
);

CREATE TABLE public.Dim_Team (
    id INT PRIMARY KEY,
    team_api_id INT,
    team_fifa_api_id FLOAT,
    team_long_name VARCHAR(255),
    team_short_name VARCHAR(255)
);

CREATE TABLE public.Dim_Team_Attributes (
    id INT PRIMARY KEY,
    team_api_id INT,
    team_fifa_api_id INT,
    "date" DATE,
    buildUpPlaySpeed INT,
    buildUpPlaySpeedClass VARCHAR(255),
    buildUpPlayDribbling FLOAT,
    buildUpPlayDribblingClass VARCHAR(255),
    buildUpPlayPassing INT,
    buildUpPlayPassingClass VARCHAR(255),
    buildUpPlayPositioningClass VARCHAR(255),
    chanceCreationPassing INT,
    chanceCreationPassingClass VARCHAR(255),
    chanceCreationCrossing INT,
    chanceCreationCrossingClass VARCHAR(255),
    chanceCreationShooting INT,
    chanceCreationShootingClass VARCHAR(255),
    chanceCreationPositioningClass VARCHAR(255),
    defencePressure INT,
    defencePressureClass VARCHAR(255),
    defenceAggression INT,
    defenceAggressionClass VARCHAR(255)
);

CREATE TABLE public.Dim_Player (
    id INT PRIMARY KEY,
    player_api_id INT,
    player_name VARCHAR(255),
    player_fifa_api_id INT,
    birthday TIMESTAMP,
    height FLOAT,
    "weight" INT
);

CREATE TABLE public.Dim_Player_Attributes (
    id INT PRIMARY KEY,
    player_fifa_api_id INT,
    player_api_id INT,
    "date" DATE,
    overall_rating DECIMAL,
    potential DECIMAL,
    crossing DECIMAL,
    finishing DECIMAL,
    heading_accuracy DECIMAL,
    short_passing DECIMAL,
    dribbling DECIMAL,
    free_kick_accuracy DECIMAL,
    ball_control DECIMAL,
    sprint_speed DECIMAL,
    reactions DECIMAL,
    stamina DECIMAL,
    strength DECIMAL,
    aggression DECIMAL,
    positioning DECIMAL,
    penalties DECIMAL
);


