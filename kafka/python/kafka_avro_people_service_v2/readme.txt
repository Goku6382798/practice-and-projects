docker up inside kafka_testingwith_simpleproducerandconfigs for the docker kafka environment
simply run the uvicorn in the environment uvicorn main:app --reload
simply run the pyconsumer with python3 pyconsumer
release the data http http POST :8000/api/people count:=50

Some important commands to work with schema with kafka
How to add a new schema using terminal

 http :8081/subjects
 http :8081/subjects/people.avro.python-value
 http :8081/subjects/people.avro.python-value/versions
 http :8081/subjects/people.avro.python-value/versions/2 -b
 http :8081/schemas/ids/2
 http :8081/config
 http POST :8081/compatibility/subjects/people.avro.python-value/versions/latest
http POST :8081/subjects/people.avro.python-value/versions \
schema="{\"type\":\"record\":,\"name\":\"Person\",\"namespace\":\"com.thecodinginterface.avrodomainevents\",\"fields\":[{\"name\":\"fullName\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"title\",\"type\":\"string\"}]}"
