import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import numpy as np

# Line to avoid following error: can't adapt type 'numpy.int64'
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)


def time_data_factory(ts_value):
    '''
    Function intended to retrieve the following information, based
    on timestamp:

    - timestamp, hour, day, week of year, month, year and weekday
    '''

    time_data = []
    timestamp = time_data.append(ts_value)
    hour = time_data.append(ts_value.hour)
    day = time_data.append(ts_value.day)
    week = time_data.append(ts_value.week)
    month = time_data.append(ts_value.hour)
    year = time_data.append(ts_value.year)
    weekday = time_data.append(ts_value.weekday())

    return time_data


def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, lines=True)

    # Defining dtypes to avoid errors related to psycopg and numpy
    song_data = df[[
        'song_id', 'title', 'artist_id', 'year', 'duration']].astype({
            'song_id': str,
            'title': str,
            'artist_id': str,
            'year': int,
            'duration': float
        })

    # insert song record
    song_data = song_data.iloc[0].tolist()
    cur.execute(song_table_insert, song_data)

    # Defining dtypes to avoid errors related to psycopg and numpy
    artist_data = df[[
        'artist_id',
        'artist_name',
        'artist_location',
        'artist_latitude',
        'artist_longitude']].astype({
            'artist_id': str,
            'artist_name': str,
            'artist_location': str,
            'artist_latitude': float,
            'artist_longitude': float})

    # insert artist record
    artist_data = artist_data.iloc[0].values.tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    df['ts'] = df.apply(
        lambda x: pd.to_datetime(x['ts'], unit='ms').to_pydatetime(), axis=1)
    t = df['ts']

    # insert time data records
    time_data = list(t.apply(lambda x: time_data_factory(x)))
    column_labels = [
        'timestamp',
        'hour',
        'day',
        'week',
        'month',
        'year',
        'weekday']

    # Creating dictionary based on time_data and column_labels
    dict_time = {}
    for idx, column in enumerate(column_labels):
        dict_time[column] = [value[idx] for value in time_data]  

    # insert time data records into dataframe
    time_df = pd.DataFrame.from_dict(dict_time)

    for i, row in time_df.iterrows():
        try:
            cur.execute(time_table_insert, list(row))
        except psycopg2.Error as e:
            print('Error found inserting user data into users table.\n')
            print(e)
            print('Data where error occurred: \n')
            print(row)

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        try:
            cur.execute(user_table_insert, row)
        except psycopg2.Error as e:
            print('Error found inserting user data into users table.\n')
            print(e)
            print('Data where error occurred: \n')
            print(row)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        try:
            cur.execute(song_select, (row.song, row.artist, row.length))
            results = cur.fetchone()
        except psycopg2.Error as e:
            print('Error found on SELECT query.\n')
            print(e)
            print('Data where error occurred: \n')
            print(row)

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (
            row['ts'], 
            row['userId'],
            row['level'],
            songid,
            artistid,
            row['sessionId'],
            row['location'],
            row['userAgent'])

        try:
            cur.execute(songplay_table_insert, songplay_data)
        except psycopg2.Error as e:
            print('Error found inserting data into songplay table.\n')
            print(e)
            print('Data where error occurred: \n')
            print(row)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
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
    try:
        conn = psycopg2.connect(
            "host=127.0.0.1 dbname=sparkifydb user=student password=student")
        cur = conn.cursor()
    except psycopg2.Error as e:
        print('Error occurred while connecting to postres.\n')
        print(e)

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
