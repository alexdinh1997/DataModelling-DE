import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    process all data from songs files
    cur: for connection cursor with inserting the data in DB
    filepath: the paths of song file
    """
    # open song file
    df = pd.read_json(filepath,lines=True)

    # insert song record
    
    song_data = df[['song_id', 'title', 'artist_id', 'year','duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    """
    song_table_insert: execute to the insert of table songs which is created in sql_queries
    song_data: the dataframe of song data generated from json file to list which identical columns
    """
    # insert artist record
    artist_data = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    process all data from log file:
    cur: cursor connection to DB
    filepath: path to log data directory
    """
    # open log file
    df = pd.read_json(filepath,lines=True)

    # filter by NextSong action
    df = df[df['page']=='NextSong']
#ANOTHER WAY:
#     t = pd.to_datetime(df.ts, unit='ms')
#     time_data = (df.ts, t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday)
#     column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
#     time_df = pd.DataFrame(dict(zip(column_labels, time_data)))
    
    # convert timestamp column to datetime
    df['datetime'] = pd.to_datetime(df['ts'], unit='ms')
    t=df
    t['year']=t['datetime'].dt.year
    t['month']=t['datetime'].dt.month
    t['day']=t['datetime'].dt.day
    t['hour']=t['datetime'].dt.hour
    t['week']=t['datetime'].dt.week
    t['weekday_name']=t['datetime'].dt.weekday_name
    t.head()
    
    # insert time data records
    time_data = ('ts','hour','day','week','month','year','weekday_name')
    column_labels = ('starting_time','hour','day','week','month','year','weekday')
    time_df = t[['ts','hour','day','week','month','year','weekday_name']]

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))
    """
    time_table_insert: access to insert time command
    list(row): is from which classfied rows list from time_df which is from log file
    """
    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
    """
    user_table_insert:
    row: data row from dataframe of logfile which identical columns
    """
    
    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        """
        song_select: access to 'Find song' queries with conditions
        row.song,row_artist,row_length: insert 3 conditions from log file to 'find song'
        
        """
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.itemInSession, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)

        """
        Select the timestamp, user ID, level, song ID, artist ID, session ID, location, and user agent and set to songplays
        and insert to songplay table with song_table_insert
        
        """
def process_data(cur, conn, filepath, func):
    """
    Process all data both songs and log files
    cur: DB connection cursor
    filepath: paths of directory
    func: fucntion for processing, transforming and inserting
    
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
