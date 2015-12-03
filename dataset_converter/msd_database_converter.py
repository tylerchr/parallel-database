import os
import sys
import glob
import time
import datetime
import numpy as np
try:
    import sqlite3
except ImportError:
    print 'you need sqlite3 installed to use this program'
    sys.exit(0)

# Columns
# [0] track_id text PRIMARY KEY,    <--
# [1] title text,                   <--
# [2] song_id text,
# [3] release text,                 <--
# [4] artist_id text,
# [5] artist_mbid text,
# [6] artist_name text,             <--
# [7] duration real,                <--
# [8] artist_familiarity real,      <--
# [9] artist_hotttnesss real,       <--
# [10] year int,                    <--
# [11] track_7digitalid int,        <--
# [12] shs_perf int,
# [13] shs_work int

# track_id
# song_id
# title
# artist_name
# artist_location
# artist_hotttnesss
# release
# year
# song_hotttnesss
# danceability
# duration
# loudness
# analysis_sample_rate
# tempo

def get_all_data(target, dbfile):
    # list all columns names from table 'songs'
    # q = "SELECT sql FROM sqlite_master WHERE tbl_name = 'songs' AND type = 'table'"
    # res = c.execute(q)
    # print res.fetchall()[0][0]
    target.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % (
        "track_id",
        "title",
        "release",
        "artist_name",
        "duration",
        "artist_familiarity",
        "artist_hotttnesss",
        "year",
        "track_7digitalid"
    ))

    target.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % (
        "string",
        "string",
        "string",
        "string",
        "float",
        "float",
        "float",
        "int",
        "int"
    ))

    # convert database into text file
    count = 0
    q = "SELECT * FROM songs"
    res = c.execute(q)
    for element in res.fetchall():
        target.write("%s\t%s\t%s\t%s\t%f\t%f\t%f\t%d\t%d\n" % (
            element[0].encode('utf-8').replace("\t", "  "),
            element[1].encode('utf-8').replace("\t", "  "),
            element[3].encode('utf-8').replace("\t", "  "),
            element[6].encode('utf-8').replace("\t", "  "),
            element[7],
            element[8],
            element[9],
            element[10],
            element[11]
        ))

        count += 1
        print "%d/1000000" % (count)


dbfile = 'track_metadata.db'        # PATH TO track_metadat.db
conn = sqlite3.connect(dbfile)      # connect to the SQLite database
c = conn.cursor()                   # from that connection, get a cursor to do queries

filename = "msd.tsv" # output filename
target = open(filename, 'w')
get_all_data(target, dbfile)

# close the cursor and the connection
c.close()
conn.close()