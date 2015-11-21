import os
import glob
import hdf5_getters

def get_all_data(target, basedir, ext='.h5') :

    # header
    target.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % (
                 "track_id", "song_id", "title", "artist_name", "artist_location",
                 "artist_hotttnesss", "release", "year", "song_hotttnesss",
                 "danceability", "duration", "loudness", "sample_rate", "tempo"
    ))

    count = 0
    for root, dirs, files in os.walk(basedir):
        files = glob.glob(os.path.join(root,'*'+ext))
        for f in files:
            for line in f:
                new_file = open("tmp.txt", 'w')
                new_file.write(line)

                h5 = hdf5_getters.open_h5_file_read(new_file)
                target.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % (
                              hdf5_getters.get_track_id(h5),
                              hdf5_getters.get_song_id(h5),
                              hdf5_getters.get_title(h5),
                              hdf5_getters.get_artist_name(h5),
                              hdf5_getters.get_artist_location(h5),
                              hdf5_getters.get_artist_hotttnesss(h5),
                              hdf5_getters.get_release(h5),
                              hdf5_getters.get_year(h5),
                              hdf5_getters.get_song_hotttnesss(h5),
                              hdf5_getters.get_danceability(h5),
                              hdf5_getters.get_duration(h5),
                              hdf5_getters.get_loudness(h5),
                              hdf5_getters.get_analysis_sample_rate(h5),
                              hdf5_getters.get_tempo(h5)
                ))

                # show progress
                count += 1
                print "%d/10000" % (count)

                h5.close()

filename = "msd.tsv"
target = open(filename, 'w')
basedir = "/home/jaredririe/Dropbox/School/2015 Fall/CS 484/Project"
get_all_data(target, basedir)