Some important commands

After the creation of kafka cluster to run the producer ## important notice if when the producer is started the consumer takes time 

sudo docker exec -it cli-tools kafka-console-producer --bootstrap-server broker0:29092 --topic people


After the creation of kafka cluster to run the consumer

sudo docker exec -it cli-tools kafka-console-consumer --bootstrap-server broker0:29092 --topic people --from-beginning

