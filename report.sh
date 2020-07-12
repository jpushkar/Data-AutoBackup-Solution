#!/bin/bash
time=`date "+%d-%m-%Y-%H-%M-%S"`
mysql --host=localhost --user=root --password=R00t@123 transfer -e "select * from status_volume2 where status = 2 INTO OUTFILE '/tmp/volume2-report-"$time".csv'  FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';" > /dev/null 2>&1
mysql --host=localhost --user=root --password=R00t@123 transfer -e "select * from status_usb where status = 2 INTO OUTFILE '/tmp/usb-report-"$time".csv'  FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';" > /dev/null 2>&1
mysql --host=localhost --user=root --password=R00t@123 transfer -e "select * from status_Qnap where status = 2 INTO OUTFILE '/tmp/Qnap-report-"$time".csv'  FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';" > /dev/null 2>&1

echo "------------------------------Report is created sucessfully. Please check files below --------------------------------"
echo "----------------------/volume2 filename is  /tmp/volume2-report-"$time".csv --------------------------------------"
echo "----------------------/usb filename is  /tmp/usb-report-"$time".csv -----------------------------------------------"
echo "----------------------/volume2 filename is /tmp/Qnap-report-"$time".csv ------------------------------------------"

