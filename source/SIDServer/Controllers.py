__author__ = 'bnelson'
import datetime as dt
import dateutil.parser
import os
import time as threadtime
import numpy as np

from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.s3.connection import OrdinaryCallingFormat

from SIDServer.Objects import File
from SIDServer.Objects import StationReading
from SIDServer.Objects import SiteSpectrumReading
from SIDServer.Objects import SiteSpectrum
from SIDServer.Utilities import HDF5Utility
from SIDServer.Utilities import DateUtility
from SIDServer.Utilities import FrequencyUtility
from SIDServer.Utilities import ZipUtility
from SIDServer.Utilities import PrintHelper
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
            PrintHelper.print('Checking for files', 1)

            connection = S3Connection(access_key, secret_key, calling_format=OrdinaryCallingFormat())
            destination_bucket = connection.get_bucket(destination_bucket_name)

            source_bucket = connection.get_bucket(source_bucket_name)
            objects = source_bucket.list()

            for key in objects:
                file_name = key.key
                working_file = temp_folder + file_name

                PrintHelper.print('Downloading {0}'.format(file_name), 1)
                key.get_contents_to_filename(working_file)

                PrintHelper.print('Processing {0}'.format(file_name))
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

                dao = DataAccessObject(self.Config)
                site = dao.get_site(monitor_id)

                if site is not None:
                    file = dao.get_file(file_name)
                    if file is None:
                        file = self.build_new_file(dao, file_name, site.Id, created_time)

                    #process the stations
                    sg_keys = stations_group.keys()
                    for sg_key in sg_keys:
                        self.process_station(dao, file, stations_group[sg_key])

                    #process the frequency spectrum
                    fsg_keys = frequency_spectrum_group.keys()
                    for fsg_key in fsg_keys:
                        self.process_site_spectrum(dao,
                                                   file,
                                                   frequency_spectrum_group,
                                                   frequency_spectrum_group[fsg_key])

                    #currently raw data isn't processed due to the size required.  It is made
                    #available in the zipped hd5 files via S3 web sharing.

                    data_file.close()

                    #send the zipped archive to the destination
                    self.send_processed_file(monitor_id, working_file, destination_bucket)

                    #delete the archive from the source
                    source_bucket.delete_key(key.key)

                    PrintHelper.print('Processing complete on {0}'.format(file_name), 1)
                else:
                    PrintHelper.print('Site {0} was not found'.format(monitor_id))
                    data_file.close()
                    #need to move to process later since site doesn't exist

                dao.DB.commit()
                dao.close()

            PrintHelper.print('Sleeping for 60 seconds', 1)
            threadtime.sleep(60)
        else:
            PrintHelper.print('Bad user or password information provided.', 4)

    def build_new_file(self, dao, file_name, site_id, created_time):
        file = File()
        file.Archived = False
        file.Available = False
        file.CreatedAt = dt.datetime.utcnow()
        file.UpdatedAt = file.CreatedAt
        file.FileName = file_name
        file.Processed = False
        file.SiteId = site_id
        file.DateTime = created_time
        dao.save_file(file)

        return file

    def send_processed_file(self, monitor_id, working_file, destination_bucket):
        if working_file is not None:
            zip_file_name = ZipUtility.zip_file_and_delete_original(working_file)

            head, tail = os.path.split(zip_file_name)

            PrintHelper.print('Starting to move file to S3 ({0})'.format(tail))
            item_key = Key(destination_bucket)
            item_key.key = '//{0}//{1}'.format(monitor_id, tail)
            item_key.set_contents_from_filename(zip_file_name)
            PrintHelper.print('Completed moving file to S3 ({0})'.format(tail))

            os.remove(zip_file_name)
            PrintHelper.print('Removed local copy of file ({0})'.format(tail))
        else:
            PrintHelper.print('Working file was not specified to zip and move ')

    def process_station(self, dao, file, group):
        callsign = group.attrs["CallSign"]
        station = dao.get_station(callsign)

        if file is not None:
            if station is not None:
                ds_keys = group.keys()

                bulk_data = []

                for sg_key in ds_keys:
                    dataset = group[sg_key]
                    time = dataset.attrs['Time']

                    rdt = dateutil.parser.parse(time)
                    signal_strength = dataset[0]

                    reading = StationReading()
                    reading.SiteId = file.SiteId
                    reading.StationId = station.Id
                    reading.ReadingDateTime = rdt
                    reading.FileId = file.Id
                    reading.CreatedAt = dt.datetime.utcnow()
                    reading.UpdatedAt = reading.CreatedAt
                    reading.ReadingMagnitude = signal_strength
                    bulk_data.append(reading.to_insert_array())

                PrintHelper.print('Processing Station {0}: Count - {1}'.format(callsign, len(bulk_data)),1)
                dao.save_many_station_reading(bulk_data)
                
                dao.DB.commit()
            else:
                PrintHelper.print('Station is not found in database')
        else:
            PrintHelper.print('File was not supplied')

    def process_site_spectrum(self, dao, file, group, dataset):
        PrintHelper.print('Processing Frequency Spectrum Dataset - {0}'.format(dataset.name), 1)

        if file is not None:
            if dataset is not None:
                time = dataset.attrs['Time']

                reading_datetime = dateutil.parser.parse(time)
                reading_datetime.replace(microsecond=0)

                site_spectrum = dao.get_site_spectrum(file.SiteId, reading_datetime)

                if site_spectrum is None:
                    site_spectrum = SiteSpectrum()
                    site_spectrum.SiteId = file.SiteId
                    site_spectrum.ReadingDateTime = time
                    site_spectrum.FileId = file.Id
                    site_spectrum.CreatedAt = dt.datetime.utcnow()
                    site_spectrum.UpdatedAt = site_spectrum.CreatedAt
                else:
                    site_spectrum.UpdatedAt = dt.datetime.utcnow()

                    PrintHelper.print("Deleting old data")
                    dao.delete_site_spectrum_data(site_spectrum.Id)
                    dao.DB.commit()
                    PrintHelper.print("Delete complete")

                site_spectrum.NFFT = int(group.attrs.get('NFFT', 1024))
                site_spectrum.SamplesPerSeconds = int(group.attrs.get('SamplingRate', 96000))
                site_spectrum.SamplingFormat = int(group.attrs.get('SamplingFormat', 24))

                PrintHelper.print("Spectrum save started")
                site_spectrum.Id = dao.save_site_spectrum(site_spectrum)
                dao.DB.commit()
                PrintHelper.print("Spectrum save complete")

                self.process_site_spectrum_data(dao, site_spectrum, dataset)
                dao.DB.commit()
            else:
                PrintHelper.print('Dataset not supplied')
        else:
           PrintHelper. print('File was not supplied')

    def process_site_spectrum_data(self, dao, site_spectrum, dataset):
        if site_spectrum is not None:
            if dataset is not None:
                shape = dataset.shape
                array = np.zeros(shape)
                dataset.read_direct(array)
                rows = shape[0]

                if rows == 2:
                    width = shape[1]

                    bulk_data = []

                    for x in range(0, width):
                        frequency = array[0, x]
                        reading_magnitude = array[1, x]

                        reading = SiteSpectrumReading()
                        reading.Id = 0
                        reading.SiteSpectrumId = site_spectrum.Id
                        reading.Frequency = frequency
                        reading.ReadingMagnitude = reading_magnitude
                        reading_data = reading.to_insert_array()
                        bulk_data.append(reading_data)

                    PrintHelper.print("Spectrum bulk insert beginning")
                    dao.save_many_site_spectrum_reading(bulk_data)
                    PrintHelper.print("Spectrum bulk insert complete")
                else:
                    PrintHelper.print('Frequency Spectrum data set not the correct shape')
            else:
                PrintHelper.print('Dataset not supplied')
        else:
            PrintHelper.print('site spectrum not supplied')

    def stop(self):
        self.Done = True
