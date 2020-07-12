#!/usr/bin/env python3.6
from __future__ import division
import time
import os
import datetime
import csv
import threading
import concurrent.futures
import mysql.connector
from os.path import join, getsize
from google.cloud import storage

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/pushkar/service-account.json"
BatchSize = 30 
e = concurrent.futures.ThreadPoolExecutor(max_workers=BatchSize)


def file_list(folder_name):
    mydb = mysql.connector.connect(host='localhost', user='xxxxx', passwd='xxxxx', db='transfer')
    cursor = mydb.cursor()
    cursor.execute("""INSERT INTO last_updated_volume2 (last_run) VALUES (now())""")
    f = open("/home/pushkar/python-code/upload-volume2/db-filelist.csv", "a+")
    f1 = open("/home/pushkar/python-code/upload-volume2/Error-filelist.csv", "a+")
    for root, dirs, files in os.walk(folder_name):
        for file in files:
            absolute_path = join(root, file)
            if "/." not in absolute_path:
                a = os.stat(absolute_path)
                #               print(type(a.st_mtime))
                d1 = datetime.datetime.fromtimestamp(a.st_mtime)
                d2 = datetime.datetime(2000, 1, 1, 00, 00, 0)
                cursor.execute('select * from last_updated_volume2 order by last_run DESC LIMIT 1,1;')
                d3 = cursor.fetchone()[0]
                print(d1, d2, d3)
                #               mod_timestamp = datetime.datetime.fromtimestamp(path.getmtime())
                #               if (a.st_mtime) < (datetime(2018,1,1,0,0,0).timestamp()):
                if d1 > d3:
                    # check if file is last modified after 2018-01-01 (d1)
                    print("date is" , d3)
                    print("---File modified after 2000-01-01 pushing it to DB---", file)
                    try:
                        f.write("%s %s\n" % (absolute_path, datetime.datetime.fromtimestamp(a.st_mtime)))
                        # cursor = mydb.cursor()
                        cursor.execute('INSERT INTO status_volume2 (file, created_time) VALUES("' + absolute_path + '", "' + d1.strftime('%Y-%m-%d %H:%M:%S') + '")')
                    except mysql.connector.errors.DatabaseError:
                        print("Error in the file name ")
                        f1.write("%s %s\n" % (absolute_path, datetime.datetime.fromtimestamp(a.st_mtime)))
                else:
                    print("---File is not modified after ignored---", file)
    mydb.commit()
    print("Done", cursor.lastrowid)
    cursor.close()
    f.close()
    f1.close()


def set_blob():
    """set file to 0"""
    mydb = mysql.connector.connect(host='localhost', user='xxxx', passwd='xxxxxxx', db='transfer')
    cursor = mydb.cursor()
    cursor.execute('update status_volume2 set status = 0 where status = 1')
    mydb.commit()
    cursor.close()


def upload_blob():
    """Uploads a file to the bucket."""
    mydb = mysql.connector.connect(host='localhost', user='xxxxxx', passwd='xxxxx', db='transfer')
    futureRes = []
    start =1
    while start == 1:
        cursor = mydb.cursor()
        cursor.execute('select count(1) from status_volume2 where status = 0')
        count = cursor.fetchone()
        print("Initail Count value is ", count[0])
        ct = count [0]
        if ct == BatchSize -1:
            print("Inside ct if--- Script is completed.. Finsishing up process")
            update_Script_completed()
            start = 0
            exit(0)
        if ct >= 1:
            rows = get_file_to_upload()
            for i in rows:
                source_file_name = i[0]
                print('File SQL: {}'.format(source_file_name))
                futureRes.append(e.submit(upload_to_storage, source_file_name, ct))  # upload_to_storage(source_file_name)
                print('Future List Size: {}'.format(len(futureRes)))
                print("Count value is ", count)
                if len(futureRes) > BatchSize -1:
                    print("inside if")
                    concurrent.futures.wait(futureRes, return_when=concurrent.futures.FIRST_COMPLETED)
                    for f in futureRes:
                        if f.done():
                            print("----------------Inside futures loop--------------------->")
                            print("Count value is ", ct)
                            futureRes.remove(f)
                            ct = ct-1
                            if ct == BatchSize -1:
                                print("----------Inside ct if--- Script is completed.. Finishing up process")
                                update_Script_completed()
                                exit(0)
        else:
            print("in the lese loop",count[0])
            update_Script_completed()
            break;

def get_file_to_upload():
    mydb = mysql.connector.connect(host='localhost', user='xxxxx', passwd='xxxxx', db='transfer')
    cursor = mydb.cursor()
    cursor.execute('SELECT file from status_volume2 where status = 0 limit 100')
    return cursor.fetchall()

def upload_to_storage(source_file_name, ct):
    mydb = mysql.connector.connect(host='localhost', user='xxxxx', passwd='xxxxx', db='transfer')
    cursor = mydb.cursor()
    cursor.execute('UPDATE status_volume2 SET status = 1 WHERE file = "' + source_file_name + '"')
    mydb.commit()
    cursor.close()
    bucket_name = "gcs-bucket-name"
    dest_dir = "dest/folder/name/"
    dest_file_name_temp = source_file_name
    upload_file_name = (dest_file_name_temp.split("/volume2/",1)[1])
    print (upload_file_name)
    destination_blob_name = dest_dir + upload_file_name
    print (bucket_name, source_file_name, destination_blob_name)
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print('File {} uploaded to {}.'.format(source_file_name, destination_blob_name))
    update_db(source_file_name)    
    print("ct value is", ct)

def update_db(filename):
    mydb = mysql.connector.connect(host='localhost', user='xxxxx', passwd='xxxxx', db='transfer')
    cursor = mydb.cursor()
    cursor.execute('UPDATE status_volume2 SET status = 2 WHERE file = "' + filename + '"')
    mydb.commit()
    cursor.close()

def check_script_Status():
    mydb = mysql.connector.connect(host='localhost', user='xxxxx', passwd='xxxxx', db='transfer')
    cursor = mydb.cursor()
    cursor.execute('select copy_finished from last_updated_volume2 order by last_run DESC LIMIT 1;')
    copy_completed = cursor.fetchone()[0]
    cursor.close()
    print(copy_completed)
    if copy_completed == 0:
        print("-----Last copy script was not completed. Starting Again..-----")
        sendmail_location = "/usr/sbin/sendmail" # sendmail location
        p = os.popen("%s -t" % sendmail_location, "w")
        p.write("From: %s\n" % "from@dom.com")
        p.write("To: %s\n" % "to@domain.com")
        p.write("Subject:  Backup : Upload volume2 script started...\n")
        p.write("\n") # blank line separating headers from body
        p.write("Last copy script was not completed. Starting Again..\n")
        set_blob()
        upload_blob()
    else:
        print("-----Last copy script was completed. Starting with new file list..-----")
        p = os.popen("%s -t" % sendmail_location, "w")
        p.write("From: %s\n" % "from@dom.com")
        p.write("To: %s\n" % "to@domain.com")
        p.write("Subject:  Backup : Upload volume2 script started...\n")
        p.write("\n") # blank line separating headers from body
        p.write("Last copy script was completed. Starting with new file list..\n")
        file_list("/volume2/")
        set_blob()
        upload_blob()

def update_Script_completed():
    mydb = mysql.connector.connect(host='localhost', user='xxxxx', passwd='xxxxx', db='transfer')
    cursor = mydb.cursor()
    cursor.execute('update last_updated_volume2 set copy_finished = 1 ORDER BY last_run DESC LIMIT 1')
    mydb.commit()
    cursor.close()
    sendmail_location = "/usr/sbin/sendmail" # sendmail location
    p = os.popen("%s -t" % sendmail_location, "w")
        p.write("From: %s\n" % "from@dom.com")
        p.write("To: %s\n" % "to@domain.com")
    p.write("Subject:  Backup : Upload volume2 script completed...\n")
    p.write("\n") # blank line separating headers from body
    status = p.close()
    print("---The script is completed at time and last_updated table is marked as 1---")
    print(datetime.datetime.today());

print("---Script started at time : ---", datetime.datetime.today())


check_script_Status()
print("---The script is completed at time and last_updated table is marked as 1---")
print(datetime.datetime.today());

