__author__ = 'bnelson'

import pymysql
import datetime as dt
import sys
from SIDServer.Objects import Station
from SIDServer.Objects import StationReading
from SIDServer.Objects import Site
from SIDServer.Objects import File
from SIDServer.Objects import SiteSpectrum
from SIDServer.Objects import SiteSpectrumReading


class DataAccessObject:

    def __init__(self, config):
        """
        Constructor
        """
        self.Config = config
        self.DB = pymysql.connect(host=self.Config.SidWatchDatabase.Host,
                                  db=self.Config.SidWatchDatabase.Database,
                                  user=self.Config.SidWatchDatabase.User,
                                  passwd=self.Config.SidWatchDatabase.Password)

    def close(self):
        self.DB.close()

    def get_file(self, filename):
        sql = """SELECT id, siteid, datetime, filename, processed, archived, available,
                       created_at, updated_at
                FROM files
                WHERE filename = %s"""

        cursor = self.DB.cursor()
        cursor.execute(sql, filename)
        row = cursor.fetchone()

        if row is not None:
            file = File()
            file.load_from_row(row)
            return file
        else:
            return None

    def save_file(self, file):
        if file.Id == 0:
            self.__insert_file__(file)
        else:
            self.__update_file__(file)

    def __insert_file__(self, file):
        sql = """INSERT INTO files (siteid, datetime, filename, processed, archived, available, created_at, updated_at)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""

        array = file.to_insert_array()

        cursor = self.DB.cursor()
        cursor.execute(sql, array)
        file.Id = cursor.lastrowid
        self.DB.commit()

    def __update_file__(self, file):
        sql = """UPDATE files
                 SET siteid = %s, datetime = %s, filename=%s, processed = %s, archived = %s,
                     available = %s, created_at = %s, updated_at = %s
                 WHERE id = %d """
        array = file.to_insert_array()
        array.append(file.Id)

        cursor = self.DB.cursor()
        cursor.execute(sql, array)
        self.DB.commit()

    def get_site_spectrum(self, site_id, reading_datetime):
        sql = """SELECT id, siteid, readingdatetime, samplespersecond, nfft, samplingformat,
                        fileid, created_at, updated_at
                 FROM sitespectrums
                 WHERE siteid = %s
                 AND ReadingDateTime = %s """

        cursor = self.DB.cursor()
        params = (site_id, reading_datetime)

        cursor.execute(sql, params)
        row = cursor.fetchone()

        if row is not None:
            site_spectrum = SiteSpectrum()
            site_spectrum.load_from_row(row)
            return site_spectrum
        else:
            return None

    def save_site_spectrum(self, site_spectrum):
        if site_spectrum.Id == 0:
            self.__insert_site_spectrum__(site_spectrum)
        else:
            self.__update_site_spectrum__(site_spectrum)

    def __insert_site_spectrum__(self, site_spectrum):
        sql = """INSERT INTO sitespectrums (siteid, readingdatetime, samplespersecond, nfft,
                                            samplingformat, fileid, created_at, updated_at)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s) """

        array = site_spectrum.to_insert_array()

        cursor = self.DB.cursor()
        cursor.execute(sql, array)
        site_spectrum.Id = cursor.lastrowid
        self.DB.commit()

    def __update_site_spectrum__(self, site_spectrum):
        sql = """UPDATE sitespectrums
                 SET siteid = %s,
                     readingdatetime = %s,
                     samplespersecond = %s,
                     nfft = %s,
                     samplingformat = %s,
                     fileid = %s,
                     created_at = %s,
                     updated_at = %s
                 WHERE id = %s """

        array = site_spectrum.to_insert_array()
        array = array + (site_spectrum.Id,)

        print(array)

        cursor = self.DB.cursor()
        cursor.execute(sql, array)
        self.DB.commit()

    def get_site_spectrum_reading(self, site_spectrum_id, frequency):
        sql = """SELECT id, sitespectrumid, frequency, readingmagnitude, created_at, updated_at
                 FROM sitespectrumdata
                 WHERE sitespectrumid = %s
                 AND frequency = %s """

        cursor = self.DB.cursor()
        params = (site_spectrum_id, frequency)
        cursor.execute(sql, params)
        row = cursor.fetchone()

        if row is not None:
            site_spectrum_reading = SiteSpectrumReading()
            site_spectrum_reading.load_from_row(row)
            return site_spectrum_reading
        else:
            return None

    def save_site_spectrum_reading(self, site_spectrum_reading):
        if site_spectrum_reading.Id == 0:
            self.__insert_site_spectrum_reading__(site_spectrum_reading)
        else:
            self.__update_site_spectrum_reading__(site_spectrum_reading)

    def __insert_site_spectrum_reading__(self, site_spectrum_reading):
        sql = """INSERT INTO sitespectrumdata (sitespectrumid, frequency, readingmagnitude, created_at, updated_at)
                 VALUES (%s, %s, %s, %s, %s) """

        try:
            array = site_spectrum_reading.to_insert_array()

            cursor = self.DB.cursor()
            cursor.execute(sql, array)
            site_spectrum_reading.Id = cursor.lastrowid
            self.DB.commit()
        except:
            print(sys.exc_info())
            raise


    def __update_site_spectrum_reading__(self, site_spectrum_reading):
        sql = """UPDATE sitespectrumdata
                 SET sitespectrumid = %s,
                     frequency = %s,
                     readingmagnitude = %s,
                     created_at = %s,
                     updated_at = %s
                 WHERE id = %s """

        array = site_spectrum_reading.to_insert_array()
        array = array + (site_spectrum_reading.Id, )

        cursor = self.DB.cursor()
        cursor.execute(sql, array)
        self.DB.commit()

    def get_station_reading(self, site_id, station_id, reading_datetime):
        sql = """SELECT id, siteid, stationid, readingdatetime, readingmagnitude, fileid, created_at, updated_at
              FROM stationreadings
              WHERE siteid = %s
              AND stationid = %s
              AND readingdatetime = %s """

        cursor = self.DB.cursor()
        params = (site_id, station_id, reading_datetime)
        cursor.execute(sql, params)
        row = cursor.fetchone()

        if row is not None:
            station_reading = StationReading()
            station_reading.load_from_row(row)
            return station_reading
        else:
            return None

    def save_station_reading(self, station_reading):
        if station_reading.Id == 0:
            self.__insert_station_reading__(station_reading)
        else:
            self.__update_station_reading__(station_reading)

    def __insert_station_reading__(self, station_reading):
        sql = """INSERT INTO stationreadings (siteid, stationid, readingdatetime, readingmagnitude, fileid,
                                            created_at, updated_at)
                 VALUES (%s, %s, %s, %s, %s, %s, %s) """
        array = station_reading.to_insert_array()

        cursor = self.DB.cursor()
        cursor.execute(sql, array)
        station_reading.Id = cursor.lastrowid
        self.DB.commit()

    def __update_station_reading__(self, station_reading):
        sql = """UPDATE stationreadings
                 SET siteid = %d, stationid = %d, readingdatetime = %s, readingmagnitude = %d,
                     fileid = %d, created_at = %s, updated_at = %s
                 WHERE Id = %d"""

        array = station_reading.to_insert_array()
        array = array + (station_reading.Id,)

        cursor = self.DB.cursor()
        cursor.execute(sql, array)
        self.DB.commit()

    def get_station(self, station_callsign):
        sql = """SELECT id, callsign, country, location, notes, frequency, latitude, longitude, created_at, updated_at
              FROM stations WHERE callsign = %s """

        cursor = self.DB.cursor()
        cursor.execute(sql, station_callsign)
        row = cursor.fetchone()

        if row is not None:
            station = Station()
            station.load_from_row(row)
            return station
        else:
            return None

    def get_site(self, monitor_id):
        sql = """SELECT id, monitorid, name, timezone, utcoffset, latitude, longitude, created_at, updated_at
              FROM sites WHERE monitorid = %s """

        cursor = self.DB.cursor()
        cursor.execute(sql, monitor_id)
        row = cursor.fetchone()

        if row is not None:
            site = Site()
            site.load_from_row(row)
            return site
        else:
            return None