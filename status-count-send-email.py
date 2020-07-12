import os
import mysql.connector
def sendMail():
    mydb = mysql.connector.connect(host='localhost', user='xxxx', passwd='xxxxxxx', db='transfer')
    cursor = mydb.cursor()
    cursor.execute('select count(1) from status_volume2 where status = 0;')
    File_yet_to_copy_volume2 = cursor.fetchone()[0]
    print(File_yet_to_copy_volume2)
    cursor.execute('select count(1) from status_volume2 where status = 1;')
    File_in_progress_volume2 =cursor.fetchone()[0]
    cursor.execute('select count(1) from status_volume2 where status = 2;')
    File_copied_volume2 =cursor.fetchone()[0]
    sendmail_location = "/usr/sbin/sendmail" # sendmail location
    p = os.popen("%s -t" % sendmail_location, "w")
    p.write("From: %s\n" % "myemail@domain.com")
    p.write("To: %s\n" % "myemail@domain.com")
    p.write("Subject:  Backup : File Upload Status\n")
    p.write("\n") # blank line separating headers from body
    p.write("/volume2 Partation Details:  :")
    p.write("\n") # blank line separating headers from body
    p.write("Total number of files remaining :" + str(File_yet_to_copy_volume2))
    p.write("\n") # blank line separating headers from body
    p.write("Current no ff Files in progress :" + str(File_in_progress_volume2))
    p.write("\n") # blank line separating headers from body
    p.write("Total Number Of Files copied:" + str(File_copied_volume2))
    cursor.execute('select count(1) from status_Qnap where status = 0;')
    File_yet_to_copy_Qnap = cursor.fetchone()[0]
    print(File_yet_to_copy_Qnap)
    cursor.execute('select count(1) from status_Qnap where status = 1;')
    File_in_progress_Qnap =cursor.fetchone()[0]
    cursor.execute('select count(1) from status_Qnap where status = 2;')
    File_copied_Qnap =cursor.fetchone()[0]
    p.write("\n") # blank line separating headers from body
    p.write("\n") # blank line separating headers from body
    p.write("/Qnap Partation Details:  :")
    p.write("\n") # blank line separating headers from body
    p.write("Total number of files remaining :" + str(File_yet_to_copy_Qnap))
    p.write("\n") # blank line separating headers from body
    p.write("Current no ff Files in progress :" + str(File_in_progress_Qnap))
    p.write("\n") # blank line separating headers from body
    p.write("Total Number Of Files copied:" + str(File_copied_Qnap))
    cursor.execute('select count(1) from status_usb where status = 0;')
    File_yet_to_copy_usb = cursor.fetchone()[0]
    print(File_yet_to_copy_usb)
    cursor.execute('select count(1) from status_usb where status = 1;')
    File_in_progress_usb =cursor.fetchone()[0]
    cursor.execute('select count(1) from status_usb where status = 2;')
    File_copied_usb =cursor.fetchone()[0]
    p.write("\n") # blank line separating headers from body
    p.write("\n") # blank line separating headers from body
    p.write("/usb Partation Details:  :")
    p.write("\n") # blank line separating headers from body
    p.write("Total number of files remaining :" + str(File_yet_to_copy_usb))
    p.write("\n") # blank line separating headers from body
    p.write("Current no ff Files in progress :" + str(File_in_progress_usb))
    p.write("\n") # blank line separating headers from body
    p.write("Total Number Of Files copied:" + str(File_copied_usb))
    p.write("\n") # blank line separating headers from body
    p.write("\n") # blank line separating headers from body
    p.write("Please check regularly, if number of file count is increasing for sucessfull backup.")
    status = p.close()
    cursor.close()
    if status != 0:
           print ("Sendmail exit status", status)

sendMail()
