__author__ = 'bnelson'

import pymysql
from SIDServer.Objects import Station
from SIDServer.Objects import Site
from SIDServer.Objects import File


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
        sql = """INSERT INTO Files (siteid, datetime, filename, processed, archived, available, created_at, updated_at)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""

        array = file.to_insert_array()
        print(array)

        cursor = self.DB.cursor()
        cursor.execute(sql, array)
        file.Id = cursor.lastrowid
        self.DB.commit()

    def save_frequency_spectrum(self, frequency_spectrum):
        pass

    def save_station_reading(self, station_reading):
        pass

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