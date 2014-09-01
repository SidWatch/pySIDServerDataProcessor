pySidServerDataProcessor
========================

The pySidServerDataProcessor scans the inbound S3 Bucket for new HDF5 files that contain data that has been uploaded from the monitoring stations.  When a new file is found the Station Readings and Frequency Spectrum data will be loaded into the MySql database so that the website has access to it.  Once the data import is completed the files will be made available at http://data.sidwatch.com.

This program is written in python so that it has the ability to utilize the NumPy, SciPy, and MatLab libraries needed to process the data that was collected by the Sid (sudden ionospheric disturbance) Monitors.

More information on the SID program can be found at http://solar-center.stanford.edu/SID/sidmonitor/.