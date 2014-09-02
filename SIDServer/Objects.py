__author__ = 'briannelson'


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