class SqlQueries:
    fact_match_select = ("""
        SELECT ID, country_id, league_id, date, match_api_id, 
        home_team_api_id, away_team_api_id, home_team_goal, away_team_goal, home_player_1, home_player_2, 
        home_player_3, home_player_4, home_player_5, home_player_6, home_player_7, home_player_8, 
        home_player_9, home_player_10, home_player_11, away_player_1, away_player_2, away_player_3, 
        away_player_4, away_player_5, away_player_6, away_player_7, away_player_8, away_player_9, 
        away_player_10, away_player_11, date_year, date_month, date_day
        FROM public.Fact_Match
    """)

    dim_weather_country_select = ("""
        SELECT date, AverageTemperature, AverageTemperatureUncertainty, Country, date_year, date_month, date_day
        FROM public.Dim_Weather_Country
    """)

    dim_country_select = ("""
        SELECT id, country_name
        FROM public.Dim_Country
    """)

    dim_league_select = ("""
        SELECT id, country_id, league_name
        FROM public.Dim_League
    """)

    dim_team_select = ("""
        SELECT id, team_api_id, team_fifa_api_id, team_long_name, team_short_name
        FROM public.Dim_Team
    """)

    dim_team_attributes_select = ("""
        SELECT id, team_api_id, team_fifa_api_id, date, buildUpPlaySpeed, buildUpPlaySpeedClass, 
        buildUpPlayDribbling, buildUpPlayDribblingClass, buildUpPlayPassing, buildUpPlayPassingClass, 
        buildUpPlayPositioningClass, chanceCreationPassing, chanceCreationPassingClass, 
        chanceCreationCrossing, chanceCreationCrossingClass, chanceCreationShooting, 
        chanceCreationShootingClass, chanceCreationPositioningClass, defencePressure, 
        defencePressureClass, defenceAggression, defenceAggressionClass
        FROM public.Dim_Team_Attributes
    """)

    dim_player_select = ("""
        SELECT id, player_api_id, player_name, player_fifa_api_id, birthday, height, weight
        FROM public.Dim_Player
    """)

    dim_player_attributes_select = ("""
        SELECT id, player_fifa_api_id, player_api_id,  date, overall_rating, potential, crossing, 
        finishing, heading_accuracy, short_passing, dribbling, free_kick_accuracy, ball_control, 
        sprint_speed, reactions, stamina, strength, aggression, positioning, penalties
        FROM public.Dim_Player_Attributes
    """)
