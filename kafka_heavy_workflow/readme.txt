all of them on are tested over ubuntu 22.04 
Run the main and consumer over different terminal 

To test simply use the below instructions:
 simply start the dockercompose using docker-compose up -d
 simply register the schema in schema using (if want to check althoug if you run the uvicorn main:app it will register the same as well)
 simply start the main.py using uvicorn main:app --reload
 simply start the pyconsumer.py with python3 pyconsumer.py 
