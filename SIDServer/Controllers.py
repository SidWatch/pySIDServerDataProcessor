__author__ = 'bnelson'
import datetime as dt
import dateutil.parser
import os
import time as threadtime

from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.s3.connection import OrdinaryCallingFormat

from SIDServer.Objects import File
from SIDServer.Utilities import HDF5Utility
from SIDServer.Utilities import DateUtility
from SIDServer.Utilities import FrequencyUtility
from SIDServer.DatabaseAccess import DataAccessObject

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

            dao = DataAccessObject(self.Config)

            for key in objects:
                file_name = key.key
                working_file = temp_folder + file_name

                print('Downloading {0}'.format(file_name))
                key.get_contents_to_filename(working_file)

                print('Processing {0}'.format(file_name))
                result = HDF5Utility.read_file(working_file)

                #Create a file record
                data_file = result["File"]
                raw_data_group = result["RawDataGroup"]
                stations_group = result["StationsGroup"]
                frequency_spectrum_group = result["FrequencySpectrumDataGroup"]

                monitor_id = data_file.attrs['MonitorId']
                created_time = dateutil.parser.parse(data_file.attrs['CreatedDateTime'])
                utc_offset = data_file.attrs['UtcOffset']
                timezone = data_file.attrs['Timezone']

                site = dao.get_site(monitor_id)

                if site is not None:
                    file = dao.get_file(file_name)
                    if file is None:
                        file = File()
                        file.Archived = False
                        file.Available = False
                        file.CreatedAt = dt.datetime.utcnow()
                        file.UpdatedAt = file.CreatedAt
                        file.FileName = file_name
                        file.Processed = False
                        file.SiteId = site.Id
                        file.DateTime = created_time

                        dao.save_file(file)

                    #process the stations
                    sg_keys = stations_group.keys()
                    for sg_key in sg_keys:
                        self.process_station(dao, monitor_id, stations_group[sg_key])

                    #process the frequency spectrum
                    fsg_keys = frequency_spectrum_group.keys()
                    for fsg_key in fsg_keys:
                        self.process_frequency_spectrum(dao, monitor_id, frequency_spectrum_group[fsg_key])

                    #key.copy(destination_bucket, '//'+ key.key, reduced_redundancy=False)

                    data_file.close()

                    os.remove(working_file)
                else:
                    data_file.close()
                    #need to move to process later since site doesn't exist

                pass

            print('Sleeping for 60 seconds')
            threadtime.sleep(60)
        else:
            print('Bad user or password information provided.')

        pass

    @staticmethod
    def process_station(dao, monitor_id, group):
        print('Processing Station - {0}'.format(group.name))

        callsign = group.attrs["CallSign"]
        print('Callsign : {0}, MonitorId : {1}'.format(callsign, monitor_id))

        station = dao.get_station(callsign)
        site = dao.get_site(monitor_id)

        if site is not None:
            if station is not None:
                print('StationId : {0}, SiteId : {1}'.format(station.Id, site.Id))

                ds_keys = group.keys()

                for sg_key in ds_keys:
                    dataset = group[sg_key]

                    time = dataset.attrs['Time']
                    signal_strength = dataset[0]



                    print('Processing Station {0}: Time - {1}: Strength - {2}'.format(callsign, time, signal_strength))
            else:
                print('Station is not found in database')
        else:
            print('Site is not found in database')


    @staticmethod
    def process_frequency_spectrum(dao, monitor_id, dataset):
        print('Processing Frequency Spectrum Dataset - {0}'.format(dataset.name))

    def stop(self):
        self.Done = True
