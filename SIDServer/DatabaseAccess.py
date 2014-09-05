__author__ = 'bnelson'

import pymysql
from SIDServer.Objects import Station
from SIDServer.Objects import Site
from SIDServer.Objects import File
from SIDServer.Objects import StationReading


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

    def get_file(self, filename):
        sql = """SELECT id, siteid, datetime, filename, processed, archived, available,
                       created_at, updated_at
                FROM Files
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
        sql = """INSERT INTO Files (siteid, datetime, filename, processed, archived, available, created_at, updated_at)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""

        array = file.to_insert_array()
        print(array)

        cursor = self.DB.cursor()
        cursor.execute(sql, array)
        file.Id = cursor.lastrowid
        self.DB.commit()

    def __update_file__(self, file):
        sql = """UPDATE Files
                 SET siteid = %s, datetime = %s, filename=%s, processed = %s, archived = %s,
                     available = %s, created_at = %s, updated_at = %s
                 WHERE id = %d """
        array = file.to_insert_array()
        array.append(file.Id)
        print(array)

        cursor = self.DB.cursor()
        cursor.execute(sql, array)
        self.DB.commit()

    def save_frequency_spectrum(self, frequency_spectrum):
        pass

    def get_station_reading(self, site_id, station_id, readingdatetime):
        sql = """SELECT id, siteid, stationid, readingdatetime, readingmagnitude, fileid, created_at, updated_at
              FROM stationreadings WHERE siteid = %s AND stationid = %s AND readingdatetime = $s """

        cursor = self.DB.cursor()
        cursor.execute(sql, site_id, station_id, readingdatetime)
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
        sql = """INSERT INTO StationReading (siteid, stationid, readingdatetime, readingmagnitude, fileid,
                                            created_at, updated_at)
                                            VALUES (%d, %d, %s, %d, %d, %s, %s) """
        array = station_reading.to_insert_array()
        print(array)

        cursor = self.DB.cursor()
        cursor.execute(sql, array)
        station_reading.Id = cursor.lastrowid
        self.DB.commit()

    def __update_station_reading__(self, station_reading):
        sql = """UPDATE StationReading
                 SET siteid = %d, stationid = %d, readingdatetime = %s, readingmagnitude = %d,
                     fileid = %d, created_at = %s, updated_at = %s
                 WHERE Id = %d"""

        array = station_reading.to_insert_array()
        array.append(station_reading.Id)

        print(array)

        cursor = self.DB.cursor()
        cursor.execute(sql, array)
        self.DB.commit()

    def get_station(self, station_callsign):
        sql = """SELECT id, callsign, country, location, notes, frequency, latitude, longitude, created_at, updated_at
              FROM Stations WHERE callsign = %s """

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
              FROM Sites WHERE monitorid = %s """

        cursor = self.DB.cursor()
        cursor.execute(sql, monitor_id)
        row = cursor.fetchone()

        if row is not None:
            site = Site()
            site.load_from_row(row)
            return site
        else:
            return None