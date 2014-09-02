__author__ = 'bnelson'
import datetime as dt
import os
import time as threadtime

from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.s3.connection import OrdinaryCallingFormat

from SIDServer.Utilities import HDF5Utility
from SIDServer.Utilities import DateUtility
from SIDServer.Utilities import FrequencyUtility

class SendToSidWatchServerController:
    def __init__(self, config):
        """
        Constructor
        """
        self.Config = config
        self.Done = True

    def start(self):
        self.Done = False

        source_bucket_name = self.Config.SidWatchServer.SourceBucketName
        destination_bucket_name = self.Config.SidWatchServer.DestinationBucketName
        access_key = self.Config.SidWatchServer.AccessKey
        secret_key = self.Config.SidWatchServer.SecretKey
        temp_folder = self.Config.SidWatchServer.TempFolder

        while not self.Done:
            print('Checking for files')

            connection = S3Connection(access_key, secret_key, calling_format=OrdinaryCallingFormat())
            destination_bucket = connection.get_bucket(destination_bucket_name)

            source_bucket = connection.get_bucket(source_bucket_name)
            objects = source_bucket.list()

            for key in objects:
                working_file = temp_folder + key.key

                print('Downloading {0}'.format(key.key))
                key.get_contents_to_filename(working_file)

                print('Processing {0}'.format(key.key))
                result = HDF5Utility.read_file(working_file)

                #Create a file record
                file = result["File"]
                raw_data_group = result["RawDataGroup"]
                stations_group = result["StationsGroup"]
                frequency_spectrum_group = result["FrequencySpectrumDataGroup"]

                #process the stations
                sg_keys = stations_group.keys()
                for sg_key in sg_keys:
                    self.process_station(stations_group[sg_key])

                #process the frequency spectrum
                fsg_keys = frequency_spectrum_group.keys()
                for fsg_key in fsg_keys:
                    self.process_frequency_spectrum(frequency_spectrum_group[fsg_key])

                #key.copy(destination_bucket, '//'+ key.key, reduced_redundancy=False)

                file.close()

                os.remove(working_file)
                pass

            print('Sleeping for 60 seconds')
            threadtime.sleep(60)
        else:
            print('Bad user or password information provided.')

        pass

    @staticmethod
    def process_station(group):
        print('Processing Station - {0}'.format(group.name))

        ds_keys = group.keys()

        for sg_key in ds_keys:
            print('Processing Station Dataset - {0}'.format(sg_key))


    @staticmethod
    def process_frequency_spectrum(dataset):
        print('Processing Frequency Spectrum Dataset - {0}'.format(dataset.name))

    def stop(self):
        self.Done = True
