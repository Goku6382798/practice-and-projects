note:- python 3.8 is required for the faust
first docker-compose up -d
second we created the topic with main.py 
then use the below one first one will check the topic and with the other one we can access and send the greetings
docker exec -it cli-tools kafka-topics --list --bootstrap-server broker0:29092
docker exec -it cli-tools kafka-console-producer --topic greetings --bootstrap-server broker0:29092