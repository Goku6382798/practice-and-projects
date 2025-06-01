first docker-compose up -d
faust -A main worker -l info
docker exec -it cli-tools kafka-topics --list --bootstrap-server broker0:29092
docker exec -it cli-tools kafka-console-consumer --topic greetings-event --from-beginning --bootstrap-server broker0:29092