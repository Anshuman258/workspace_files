#!/bin/sh

docker_image_id=""
running_docker_image=""
new_docker_image=""
operation="abc"

echo "$operation"

while getopts d:r:n:p: option
do
case "${option}"
in
d) docker_image_id=${OPTARG};;
r) running_docker_image=${OPTARG};;
n) new_docker_image=${OPTARG};;
p) operation=${OPTARG};;
esac
done

echo "$docker_image_id"
echo "$running_docker_image"
echo "$new_docker_image"
echo "$operation"

time_stamp=$(date +%Y-%m-%d-%T)

#cd ~/Documents/workspace
#mkdir "logs$time_stamp"
#cd "logs$time_stamp"
#
#echo "copying logs"
#sudo docker cp $docker_image_id:/var/log/enable_backend .
#echo "log copied"

if [ "$operation" = "docker_compose_update" ]
then
  cd BASE_PATH
  echo "current path"
  pwd
  SS=$running_docker_image
  TT=$new_docker_image
  echo "changing docker-compose"
  sed -e "s,$SS,$TT,g" docker-compose.yml > output.txt
  cp output.txt docker-compose.yml
  rm output.txt
  echo "docker-compose updation finished"
fi


if [ "$operation" = "docker_container_restart" ]
then
  cd BASE_PATH
  echo "current path"
  pwd
  echo "stopping old docker image"
  sudo docker-compose down
  echo "stopped old docker image"
  sleep 15
  echo "running new docker image"
  sudo docker-compose up -d
  echo "new docker is running"
fi


#cd ~/Documents/ota_update
#echo "current path"
#pwd
#SS=$running_docker_image
#TT=$new_docker_image
#echo "changing docker-compose"
#sed -e "s,$SS,$TT,g" docker-compose.yml > output.txt
#cp output.txt docker-compose.yml
#rm output.txt
#echo "docker-compose updation finished"
#
#cat docker-compose.yml
#
#echo "stopping old docker image"
#sudo docker-compose down
#echo "stopped old docker image"
#sleep 15
#echo "running new docker image"
#sudo docker-compose up -d
#echo "new docker is running"

# cat docker-compose.yml
