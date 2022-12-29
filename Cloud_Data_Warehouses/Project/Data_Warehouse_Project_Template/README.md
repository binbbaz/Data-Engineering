The purpose of this database is to move current sparkify's processes onto the cloud by staging their data from S3 bucket to a database hosted on 
Redshift. The idea is to be able to build and ETL pipeline that allows transformation of the data into a set of dimensional
tables for Sparkify's analystical team to continue finding insights into what songs their users are listening to and make 
further analytics.

The database schema design has five (7) tables. Two of these tables (1 and 2) hold the existing data copied from the S3
bucket before  staging them into the other tables (3-7)

1. staging_events: (artists, auth, firstName, gender, itemInSession, lastName, length, level, location,method, page, registration, sessionId, song, status, ts, userAgent, userId)
2. staging_songs: (artist_id, artist_latitude, artist_location, artist_longitude, artist_name, duration, num_songs, song_id, title, year)
3. songplays : (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
4. users: (user_id, first_name, last_name, gender, level)
5. songs: (song_id, title, artist_id, year, duration)
6. artist: (artist_id, name, location, lattitude, longitude)
7. time: (start_time, hour, day, week, month, year, weekday)

The S3 links to the datasets (Song and Log dataset) are in the file, dwh.cfg in thesame directory. 

Note: MAKE SURE THAT THE S3 LINKS IN THE dwh.cfg FILE ARE CORRECT. I SPENT MORE THAN A WEEK FIXING ERRORS AS A RESULT OF WRITING '-' instead of '_' where unncessary