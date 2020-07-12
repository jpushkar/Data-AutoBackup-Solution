#!/bin/bash


ps=`ps -ef | grep upload-volume2-17Sep.py | grep -v grep|awk -F " " '{print $2}' | wc -l`
	if [ $ps -eq 0 ]
	then
		echo "Script is not running. Restarting the script..."
		cd /home/pushkar/python-code/upload-volume2/
		nohup /usr/local/bin/python3.6 /home/pushkar/python-code/upload-volume2/upload-volume2-17Sep.py &
		echo "Mumbai Backup : /volume2 upload script was not running started again.." | mail -s "Mumbai Backup: /volume2  " it.india@vuclip.com,pushkar.joshi@vuclip.com,suyog.shirgaonkar@vuclip.com,nirav.shah@vuclip.com 
	else
		echo "Script is running"
	fi
