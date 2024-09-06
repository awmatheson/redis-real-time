# Real-Time ML Platform on Redis

Redis provides a datastore with many different functionalities that makes it great for real-time ML capabilities. It provides a streaming interface for streaming ingestion and consumption. It also provides the key value store as well as a vector database that are very well suited for both classical ML problems as well as generative AI applications

This repo contains an example of how you can use Bytewax and Redis together to build a Real-time ML platform.

`src/dataflow_input.py` is an ingestion script to generate a continuous stream of data.

`src/dataflow_transform.py` is a transformation pipeline to turn data into features and store it in redis for inference time.

# Run the platform

`pip install redis bytewax numpy`

`docker run -d --name redis-stack -p 6379:6379 -p 8001:8001 redis/redis-stack:latest`

`python -m bytewax.run dataflow_input.py`

`python -m bytewax.run dataflow_transform.py`

`docker exec -it redis-stack redis-cli`

`redis> get 'metric-key`
