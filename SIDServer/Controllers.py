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
                print('Processing {0}'.format(key.key))
                key.get_contents_to_filename(temp_folder + key.key)
                





                #key.copy(destination_bucket, '//'+ key.key, reduced_redundancy=False)


                pass


            print('Sleeping for 60 seconds')
            threadtime.sleep(60)
        else:
            print('Bad user or password information provided.')

        pass

    def stop(self):
        self.Done = True
