#!/bin/bash
SERVICE_BG="process_tasks"
SERVICE="runserver"
IMAGE_NAME="ancestor=enablebox/access-online:prod-76-rc1"
sleep 5m 
if ps ax | grep -v grep | grep -v $0 | grep $SERVICE_BG > /dev/null
	then
	    echo "$SERVICE_BG running"
	else
	    echo "$SERVICE_BG is not running, restarting docker container"
	    docker_id=""
	    docker_id=$(sudo docker ps -aqf "$IMAGE_NAME")
	    if [[ ${#docker_id} -gt 0 ]]
	    then
	      restart_container=$(sudo docker container restart "$docker_id")
	    else
	      sudo docker-compose -f /home/getmyparking/Documents/workspace/docker-compose.yml up -d> /dev/null
	    fi
fi

if ps ax | grep -v grep | grep -v $0 | grep $SERVICE > /dev/null
	then
		echo "$SERVICE service running"
		
	else
		echo "$SERVICE is not running, restarting docker container"
	    	docker_id=""
	    	docker_id=$(sudo docker ps -aqf "$IMAGE_NAME")
	    	if [[ ${#docker_id} -gt 0 ]]
	    	then
	      	  restart_container=$(sudo docker container restart "$docker_id")
	    	else
	      	  sudo docker-compose -f /home/getmyparking/Documents/workspace/docker-compose.yml up -d> /dev/null
	    	fi
fi
