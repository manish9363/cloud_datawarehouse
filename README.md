# Introduction

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

This project builds an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. The database and ETL pipeline is tested by running queries on the data.

# Data

Log Data
The JSON logs on user activity have the following structure.

Song Data
Below is an example of what a single song file, TRAABJL12903CDCF1A.json looks like.

Sample Example:

{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null,
"artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud",
"song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration":
152.92036, "year": 0}


# Schema
Star schema optimized for queries on song play analysis. This includes the following tables.

    Fact Table
        songplays		

    Dimension Tables
        users
        artists
        time
        songs
        
        
# Configuration and Prerequiste
Set up a config file dwh.cfg that uses the following schema. 
Run the Redshift Cluster.

# Procesing Steps:


    1- run Create tables script 
            python create_tables.py
            
    2- Load data into database        
        python etl.py
        
    3- Run test queries
        python test_queries.py