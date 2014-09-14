__author__ = 'bnelson'
from SIDServer import Objects
import io
import yaml
import datetime as dt
import math
import numpy as np
from matplotlib.mlab import psd
import h5py
import os
import zipfile


class ConfigUtility:
    def __init__(self):
        """
        Constructor
        """

    @staticmethod
    def load(filename):
        stream = io.open(filename, mode="r")

        config_dictionary = yaml.load(stream)

        config = Objects.Config(config_dictionary)

        return config


class HDF5Utility:
    def __init__(self):
        """
        Constructor
        """

    @staticmethod
    def add_raw_data_set(group, name, time, data_type, sample_data):
        ds = group.create_dataset(name, (len(sample_data), ), dtype=data_type, data=sample_data)
        ds.attrs["Time"] = time.isoformat()

        return ds

    @staticmethod
    def close_file(file):
        file.flush()
        file.close()

    @staticmethod
    def open_file(filename, config):
        site_config = config.Site
        audio_config = config.Audio

        data_file = h5py.File(filename, "a")

        raw_data_group = None
        frequency_spectrum_data_group = None
        stations_group = None

        if config.SidWatch.SaveRawData:
            raw_data_group = data_file.get("raw_sid_data")
            if raw_data_group is None:
                raw_data_group = data_file.create_group("raw_sid_data")
                raw_data_group.attrs["StationName"] = site_config.Name
                raw_data_group.attrs["MonitorId"] = site_config.MonitorId
                raw_data_group.attrs["Latitude"] = site_config.Latitude
                raw_data_group.attrs["Longitude"] = site_config.Longitude
                raw_data_group.attrs["UtcOffset"] = site_config.UtcOffset
                raw_data_group.attrs["Timezone"] = site_config.Timezone
                raw_data_group.attrs["SamplingRate"] = audio_config.SamplingRate
                raw_data_group.attrs["SamplingFormat"] = audio_config.SamplingFormat

        if config.SidWatch.SaveFrequencies:
            frequency_spectrum_data_group = data_file.get("frequency_spectrum_data")
            if frequency_spectrum_data_group is None:
                frequency_spectrum_data_group = data_file.create_group("frequency_spectrum_data")

        if config.SidWatch.SaveStationData:
            stations_group = data_file.get("monitored_stations")
            if stations_group is None:
                stations_group = data_file.create_group("monitored_stations")

                for station in config.Stations:
                    station_group = stations_group.get(station.CallSign)

                    if station_group is None:
                        station_group = stations_group.create_group(station.CallSign)
                        station_group.attrs["CallSign"] = station.CallSign
                        station_group.attrs["Color"] = station.Color
                        station_group.attrs["Frequency"] = station.Frequency
                        station_group.attrs["MonitoredBin"] = station.MonitoredBin

        return { "File": data_file,
                 "RawDataGroup": raw_data_group,
                 "StationsGroup": stations_group,
                 "FrequencySpectrumDataGroup": frequency_spectrum_data_group }

    @staticmethod
    def add_signal_strength(station, stations_group, dataset_name, time, signal_strength):
        station_group = stations_group.get(station.CallSign)

        if station_group is not None:
            name = time.isoformat
            ds = station_group.create_dataset(dataset_name, (1, ), data=signal_strength)
            ds.attrs["Time"] = time.isoformat()

    @staticmethod
    def add_frequency_spectrum(frequency_spectrum_data_group, dataset_name, time, frequencies, Pxx):
        joined_array = np.vstack([frequencies.real, Pxx])

        ds = frequency_spectrum_data_group.create_dataset(dataset_name,
                                                          shape=(2, len(frequencies)),
                                                          dtype=np.float64,
                                                          data=joined_array)
        ds.attrs["Time"] = time.isoformat()

    @staticmethod
    def read_file(filename):
        data_file = h5py.File(filename, "a")

        raw_data_group = data_file.get("raw_sid_data")
        frequency_spectrum_data_group = data_file.get("frequency_spectrum_data")
        stations_group = data_file.get("monitored_stations")

        return {"File": data_file,
                "RawDataGroup": raw_data_group,
                "StationsGroup": stations_group,
                "FrequencySpectrumDataGroup": frequency_spectrum_data_group}


class FrequencyUtility:
    def __init__(self):
        """
        Constructor
        """

    @staticmethod
    def process_psd(data, nfft=1024, audio_sampling_rate=96000):
        """

        :param data:
        :param nfft: Nonequispaced FFT
        :param audio_sampling_rate:
        :return:
        """
        return psd(data, nfft, audio_sampling_rate)


class DateUtility:
    def __init__(self):
        """
        Constructor
        """

    @staticmethod
    def get_next_run_time(current_date_time):
        last_seconds = int(math.floor(current_date_time.second / 5)) * 5

        last_time = dt.datetime(current_date_time.year,
                                current_date_time.month,
                                current_date_time.day,
                                current_date_time.hour,
                                current_date_time.minute,
                                last_seconds)

        return last_time + dt.timedelta(0, 5)


class ZipUtility:

    @staticmethod
    def zip_file_and_delete_original(file_to_zip):

        zip_archive_name = ""

        if file_to_zip.endswith(".h5"):
            zip_archive_name = file_to_zip.replace(".h5", ".zip")
        else:
            zip_archive_name = file_to_zip + ".zip"

        head, tail = os.path.split(zip_archive_name)

        zip_archive = zipfile.ZipFile(zip_archive_name, 'w')
        zip_archive.write(file_to_zip, tail)
        zip_archive.close()

        os.remove(file_to_zip)

        return zip_archive_name


