__author__ = 'briannelson'
import numpy as np

class Logging:
    def __init__(self, values_dictionary):
        """
        Constructor
        """

        self.FilenameFormat = values_dictionary["FilenameFormat"]
        self.Folder = values_dictionary["Folder"]
        self.TraceLevel = values_dictionary["TraceLevel"]


class Config:
    def __init__(self, values_dictionary):
        """
        Constructor
        """

        self.Logging = Logging(values_dictionary["Logging"])
        self.SidWatchServer = SidWatchServer(values_dictionary["SidWatchServer"])
        self.SidWatchDatabase = SidWatchDatabase(values_dictionary["SidWatchDatabase"])

        pass


class SidWatchServer:
    def __init__(self, values_dictionary):
        """
        Constructor
        :param values_dictionary:
        :return:
        """

        self.SourceBucketName = values_dictionary["SourceBucketName"]
        self.DestinationBucketName = values_dictionary["DestinationBucketName"]
        self.AccessKey = values_dictionary["AccessKey"]
        self.SecretKey = values_dictionary["SecretKey"]
        self.TempFolder = values_dictionary["TempFolder"]

        self.NFFT = values_dictionary["NFFT"]
        if (self.NFFT is None) or (self.NFFT == 0):
            self.NFFT = 1024


class SidWatchDatabase:
    def __init__(self, values_dictionary):
        """
        Constructor
        :param values_dictionary:
        :return:
        """

        self.Host = values_dictionary["Host"]
        self.Database = values_dictionary["Database"]
        self.User = values_dictionary["User"]
        self.Password = values_dictionary["Password"]


class Station:
    def __init__(self):
        """
        Constructor
        :param row:
        :return:
        """
        self.Id = 0
        self.Callsign = None
        self.Country = None
        self.Location = None
        self.Notes = None
        self.Frequency = 0
        self.Latitude = None
        self.Longitude = None
        self.CreatedAt = None
        self.UpdatedAt = None

    def load_from_row(self, row):
        self.Id = row[0]
        self.Callsign = row[1]
        self.Country = row[2]
        self.Location = row[3]
        self.Notes = row[4]
        self.Frequency = row[5]
        self.Latitude = row[6]
        self.Longitude = row[7]
        self.CreatedAt = row[8]
        self.UpdatedAt = row[9]


class StationReading:
    def __init__(self):
        self.Id = 0
        self.SiteId = 0
        self.ReadingDateTime = None
        self.StationId = 0
        self.ReadingMagnitude = None
        self.CreatedAt = None
        self.UpdatedAt = None

    def load_from_row(self, row):
        self.Id = row[0]
        self.SiteId = row[1]
        self.ReadingDateTime = row[2]
        self.StationId = row[3]
        self.ReadingMagnitude = row[4]
        self.CreatedAt = row[5]
        self.UpdatedAt = row[6]


class Site:
    def __init__(self):
        self.Id = 0
        self.MonitorId = None
        self.Name = None
        self.Timezone = None
        self.UtcOffset = None
        self.Latitude = None
        self.Longitude = None
        self.CreatedAt = None
        self.UpdatedAt = None

    def load_from_row(self, row):
        self.Id = row[0]
        self.MonitorId = row[1]
        self.Name = row[2]
        self.Timezone = row[3]
        self.UtcOffset = row[4]
        self.Latitude = row[5]
        self.Longitude = row[6]
        self.CreatedAt = row[7]
        self.UpdatedAt = row[8]


class File:
    def __init__(self):
        self.Id = 0
        self.SiteId = 0
        self.DateTime = None
        self.FileName = None
        self.Processed = False
        self.Archived = False
        self.Available = False
        self.CreatedAt = None
        self.UpdatedAt = None

    def load_from_row(self, row):
        self.Id = row[0]
        self.SiteId = row[1]
        self.DateTime = row[2]
        self.Processed = row[3]
        self.Archived = row[4]
        self.Available = row[5]
        self.CreatedAt = row[6]
        self.UpdatedAt = row[7]

    def to_insert_array(self):
        array = (self.SiteId,
                 self.DateTime,
                 self.FileName,
                 self.Processed,
                 self.Archived,
                 self.Available,
                 self.CreatedAt,
                 self.UpdatedAt)

        return array

class StationReading:
    def __init__(self):
        self.Id = 0
        self.SiteId = 0
        self.StationId = 0
        self.ReadingDateTime = None
        self.ReadingMagnitude = None
        self.FileId = 0
        self.CreatedAt = None
        self.UpdatedAt = None

    def load_from_row(self, row):
        self.Id = row[0]
        self.SiteId = row[1]
        self.StationId = row[2]
        self.ReadingDateTime = row[3]
        self.ReadingMagnitude = row[4]
        self.FileId = row[5]
        self.CreatedAt = row[6]
        self.UpdatedAt = row[7]

    def to_insert_array(self):
        array = (self.SiteId,
                 self.StationId,
                 self.ReadingDateTime,
                 np.asscalar(np.float64(self.ReadingMagnitude)),
                 self.FileId,
                 self.CreatedAt,
                 self.UpdatedAt)

        return array


class SiteSpectrumReading:
    def __init__(self):
        self.Id = 0
        self.SiteSpectrumId = 0
        self.Frequency = 0.0
        self.ReadingMagnitude = 0.0
        self.CreatedAt = None
        self.UpdatedAt = None

    def load_from_row(self, row):
        self.Id = row[0]
        self.SiteSpectrumId = row[1]
        self.Frequency = row[2]
        self.ReadingMagnitude = row[3]
        self.CreatedAt = row[4]
        self.UpdatedAt = row[5]

    def to_insert_array(self):
        array = (self.SiteSpectrumId,
                 np.asscalar(np.float64(self.Frequency)),
                 np.asscalar(np.float64(self.ReadingMagnitude)),
                 self.CreatedAt,
                 self.UpdatedAt)

        return array


class SiteSpectrum:
    def __init__(self):
        self.Id = 0
        self.SiteId = 0
        self.ReadingDateTime = None
        self.SamplesPerSeconds = 0
        self.NFFT = 0
        self.SamplingFormat = 0
        self.FileId = 0
        self.CreatedAt = None
        self.UpdatedAt = None

    def load_from_row(self, row):
        self.Id = row[0]
        self.SiteId = row[1]
        self.ReadingDateTime = row[3]
        self.SamplesPerSeconds = row[4]
        self.NFFT = row[5]
        self.SamplingFormat = row[6]
        self.FileId = row[7]
        self.CreatedAt = row[8]
        self.UpdatedAt = row[9]

    def to_insert_array(self):
        array = (self.SiteId,
                 self.ReadingDateTime,
                 self.SamplesPerSeconds,
                 self.NFFT,
                 self.SamplingFormat,
                 self.FileId,
                 self.CreatedAt,
                 self.UpdatedAt)

        return array