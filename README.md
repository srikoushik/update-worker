# update-worker
A python3 worker application to read the CSV dataset with Apache Spark, update to Redis and consume the data via REST APIs with Flask. Each module will run in a Docker.

To run the application ```docker-compose up``` 

# REST APIs

1. Get items based on entered color ```http://0.0.0.0:5000/items/color/Black```

2. Get recent item for the entered date(yyyy-mm-dd). ```http://0.0.0.0:5000/items/recent/2019-05-01```

3. Get items count for the entered date(yyyy-mm-dd). ```http://0.0.0.0:5000/items/count/2019-05-01```
