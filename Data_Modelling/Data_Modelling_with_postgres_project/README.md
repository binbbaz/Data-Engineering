The purpose of this database is to model user activity data
for a music streaming app called sparkify. 

Four dimension tablesand a fact table were created and populated to keep records

Dimenstion Tables or Entities

1.  users - users in the app
    atributees - user_id, first_name, last_name, gender, level

2.  songs - songs in music database
    atributes - song_id, title, artist_id, year, duration

3.  artists - artists in music database
    attributes - artist_id, name, location, latitude, longitude

4.  time - timestamps of records in songplays broken down into specific units
    attributes - start_time, hour, day, week, month, year, weekday

Fact Table
1.  songplays - records in log data associated with song plays i.e. records with page NextSong
    Attributes - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

- The data file in the project contains the log_data and song_data folder which contains
the data populated into the databse across all entities

1.  The test.ipynb displays the first few rows of each table to let you check your database.

2.  create_tables.py drops and creates the tables. THis file can be run to reset the tables before 
    each time you run your ETL scripts.

3.  etl.ipynb reads and processes a single file from song_data and log_data and loads the data into the tables. 
    This notebook contains detailed instructions on the ETL process for each of the tables.

4.  etl.py reads and processes files from song_data and log_data and loads them into the tables. 

5.  sql_queries.py contains all the sql queries used in this project, and is imported into the last three files above.

-How to run the project.

1.  Point the connection in create_table.py and etl.py to your database and edit accordingly
2.  run "python create_tables.py"
3.  run "python etl.py"
4.  confirm successful runs by running the cells in test.ipynb
