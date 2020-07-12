#!/usr/bin/python

###########################################################
#
# This python script is used for mysql database backup
# using mysqldump and tar utility and upload it to GCP bucket and delete from source
#
# Created date: AUG 27, 2019
#
##########################################################

# Import required python libraries

import os
import time
import datetime
import pipes
from google.cloud import storage
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/pushkar/mumbai-service-account.json"


# MySQL database details to which backup to be done. Make sure below user having enough privileges to take databases backup.
# To take multiple databases backup, create any file like /backup/dbnames.txt and put databases names one on each line and assigned to DB_NAME variable.
DB_HOST = 'localhost'
DB_USER = 'root'
DB_USER_PASSWORD = 'R00t@123'
#DB_NAME = '/backup/dbnameslist.txt'
DB_NAME = 'transfer'
BACKUP_PATH = '/opt/mysql-backup'

# Getting current DateTime to create the separate backup folder like "20180817-123433".
DATETIME = time.strftime('%Y%m%d-%H%M%S')
#TODAYBACKUPPATH = BACKUP_PATH + '/' + DATETIME
TODAYBACKUPPATH = BACKUP_PATH

# Checking if backup folder already exists or not. If not exists will create it.
try:
    os.stat(TODAYBACKUPPATH)
except:
    os.mkdir(TODAYBACKUPPATH)

# Code for checking if you want to take single database backup or assinged multiple backups in DB_NAME.
print ("checking for databases names file.")
if os.path.exists(DB_NAME):
    file1 = open(DB_NAME)
    multi = 1
    print ("Databases file found...")
    print ("Starting backup of all dbs listed in file " + DB_NAME)
else:
    print ("Databases file not found...")
    print ("Starting backup of database " + DB_NAME)
    multi = 0

# Starting actual database backup process.
if multi:
   in_file = open(DB_NAME,"r")
   flength = len(in_file.readlines())
   in_file.close()
   p = 1
   dbfile = open(DB_NAME,"r")

   while p <= flength:
       db = dbfile.readline()   # reading database name from file
       db = db[:-1]         # deletes extra line
       dumpcmd = "mysqldump -h " + DB_HOST + " -u " + DB_USER + " -p" + DB_USER_PASSWORD + " " + db + " > " + pipes.quote(TODAYBACKUPPATH) + "/" + db + DATETIME + ".sql"
       os.system(dumpcmd)
       gzipcmd = "gzip " + pipes.quote(TODAYBACKUPPATH) + "/" + db + DATETIME + ".sql"
       os.system(gzipcmd)
       p = p + 1
   dbfile.close()
else:
   db = DB_NAME
   dumpcmd = "mysqldump -h " + DB_HOST + " -u " + DB_USER + " -p" + DB_USER_PASSWORD + " " + db + " > " + pipes.quote(TODAYBACKUPPATH) + "/" + db + DATETIME + ".sql"
   os.system(dumpcmd)
   gzipcmd = "gzip " + pipes.quote(TODAYBACKUPPATH) + "/" + db + DATETIME + ".sql"
   os.system(gzipcmd)
backupfilename = (TODAYBACKUPPATH) + "/" + db + DATETIME + ".sql" + ".gz"
print ("")
print ("Backup script completed, starting upload to gcp bucket")
print ("Your backups have been created in '" + TODAYBACKUPPATH + "' directory and the full file name is '" + backupfilename +"'")

def upload_to_storage(backupfilename):
    bucket_name = "vuclip-originals-broadcast"
    dest_dir = "mysql-backup"
    dest_file_name_temp = backupfilename
    upload_file_name = (dest_file_name_temp.split("/opt/mysql-backup")[1])
    print(upload_file_name)
    destination_blob_name = dest_dir + upload_file_name
    print (bucket_name, backupfilename, destination_blob_name)
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(backupfilename)
    print('File {} uploaded to {}.'.format(backupfilename, destination_blob_name))
    os.remove(backupfilename)
    print(backupfilename, 'is deleted from local ')


upload_to_storage(backupfilename)

