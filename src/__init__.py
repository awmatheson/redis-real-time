"""Redis Connectors for Bytewax."""
import uuid
import logging

import redis
from typing import List, Optional, Iterable
from datetime import datetime, timezone, timedelta
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax.outputs import StatelessSinkPartition, DynamicSink

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class _RedisStreamPartition(StatefulSourcePartition):
    def __init__(self,
                 host,
                 port,
                 db,
                 stream_name,
                 batch_size: int,
                 wait_for_duration: int,
                 resume_id=None):
        self.host = host
        self.port = port
        self.db = db
        self.stream_name = stream_name
        self.batch_size = batch_size
        self.wait_for_duration = wait_for_duration
        self.resume_id = resume_id or '0-0'
        self.redis_conn = redis.Redis(host=self.host, port=self.port, db=self.db)

    def next_batch(self) -> Iterable:
        """
        Fetch the next batch of messages from the Redis stream.

        This reads a batch of messages from the Redis stream, starting 
        from the current `resume_id`. It blocks for a specified duration 
        if no messages are immediately available.

        :return: An iterable of messages from the Redis stream.
        """
        messages = self.redis_conn.xread(
            streams={self.stream_name: self.resume_id},
            count=self.batch_size,  # Adjust batch size as needed
            block=self.wait_for_duration  # Block for duration if no messages
        )
        if messages != []:
            self.resume_id = messages[0][-1][-1][0]
            return messages[0][-1]
        else:
            return []

    def snapshot(self) -> str:
        """
        Snapshot the current position in the stream.

        This returns the ID of the last processed message in the Redis stream, 
        which can be used to resume processing from this point in case of restart.

        :return: The ID of the last processed message as a string.
        """
        return self.resume_id

    def close(self) -> None:
        """
        Cleanup resources when the partition is closed.

        :return: None
        """
        self.redis_conn.close()

class RedisStreamSource(FixedPartitionedSource):
    """Read from a Redis Stream. 
    
    Limited to a single partition. 
    At-least-once possible if recovery enabled.
    """
    def __init__(
                self,
                host: str,
                port: int,
                db: int,
                stream_name: str,
                batch_size: int = 100,
                wait_for_duration: int = 10):
        """Initialize the RedisStreamSource.

        :param host: Redis server hostname.
        :param port: Redis server port.
        :param db: Redis database index.
        :param stream_name: Name of the Redis stream to read from.
        :param batch_size: Number of messages to read in each batch.
        :param wait_for_duration: Time to block waiting for messages 
                                  if none are available (milliseconds).
        """
        self.host = host
        self.port = port
        self.db = db
        self.stream_name = stream_name
        self.batch_size = batch_size
        self.wait_for_duration = wait_for_duration

    def list_parts(self) -> List[str]:
        """List all available partitions for the Redis stream source.

        Since this example uses a single Redis stream, only one partition 
        is returned as "singleton".

        :return: A list of available partitions, in this case, a single 
                 partition "singleton".
        """
        return ["singleton"]

    def build_part(self, step_id: str, for_part: str, resume_state: Optional[str]) -> _RedisStreamPartition:
        """Build a partition for reading from the Redis stream.

        Constructs and returns a partition for reading data from the Redis stream.
        It accepts a `resume_state` parameter, which is the message ID to resume reading from.

        :param step_id: The ID of the current dataflow step.
        :param for_part: The partition being built, always "singleton".
        :param resume_state: The ID of the last processed message to resume from.
        :return: A `_RedisStreamPartition` object for reading from the Redis stream.
        """
        return _RedisStreamPartition(
            self.host,
            self.port,
            self.db,
            self.stream_name,
            self.batch_size,
            self.wait_for_duration,
            resume_state
            )

class _RedisStreamSinkPartition(StatelessSinkPartition):
    def __init__(self, stream_name: str, host="localhost", port=6379, db=0):
        """
        Initialize a Redis stream sink partition.

        :param stream_name: The name of the Redis stream to write to.
        :param host: Redis server hostname.
        :param port: Redis server port.
        :param db: Redis database index.
        """
        self.stream_name = stream_name
        self.redis_conn = redis.StrictRedis(host=host, port=port, db=db)

    def write_batch(self, items: List[dict]):
        """
        Write a batch of items to the Redis stream.

        This method uses the Redis `XADD` command to add items to the Redis stream.
        Each item in the batch is a dictionary that is written as a message to the stream.

        :param items: A list of dictionaries, where each dictionary contains key-value 
                      pairs to be written as fields to the Redis stream.
        """
        for item in items:
            self.redis_conn.xadd(self.stream_name, item, id='*', maxlen=None, approximate=True)

    def close(self):
        """
        Close the Redis connection (optional).

        This closes the Redis connection when the partition is finished. 
        This is optional and may not always be called.
        """
        self.redis_conn.close()


class RedisStreamSink(DynamicSink):
    def __init__(self, stream_name: str, host="localhost", port=6379, db=0):
        """
        Initialize the Redis stream sink.

        :param stream_name: The name of the Redis stream to write to.
        :param host: Redis server hostname.
        :param port: Redis server port.
        :param db: Redis database index.
        """
        self.stream_name = stream_name
        self.redis_host = host
        self.redis_port = port
        self.redis_db = db

    def build(self, step_id: str, worker_index: int, worker_count: int) -> _RedisStreamSinkPartition:
        """
        Build a stateless sink partition for each worker.

        This method creates a `_RedisStreamSinkPartition` for each worker in the dataflow.
        Each worker will independently write messages to the Redis stream.

        :param step_id: The ID of the current dataflow step.
        :param worker_index: The index of the current worker.
        :param worker_count: The total number of workers in the dataflow.
        :return: A `_RedisStreamSinkPartition` object for writing data to the Redis stream.
        """
        return _RedisStreamSinkPartition(
            stream_name=self.stream_name,
            host=self.redis_host,
            port=self.redis_port,
            db=self.redis_db
        )

class _RedisKVSinkPartition(StatelessSinkPartition):
    def __init__(self, redis_host="localhost", redis_port=6379, redis_db=0):
        # Establish connection to Redis
        self.redis_conn = redis.Redis(host=redis_host, port=redis_port, db=redis_db)

    def write_batch(self, items):
        """
        Write a batch of key-value pairs to Redis using pipelines.

        This writes a batch of key-value pairs to Redis.
        It uses Redis pipelines to send multiple commands in
        a single round-trip for efficiency.

        :param items: A list of tuples where each tuple contains a key 
                      and its corresponding value.
        """
        print(items)
        pipe = self.redis_conn.pipeline()
        for key, value in items:
            pipe.set(key, value)
        pipe.execute()
    
    def close(self):
        """Close the Redis connection"""
        self.redis_conn.close()

class RedisKVSink(DynamicSink):
    def __init__(self, 
                 host:str = "localhost", 
                 port:int = 6379, 
                 db:int = 0):
        """
        Initialize the Redis key-value dynamic sink.

        :param host: Redis server hostname.
        :param port: Redis server port.
        :param db: Redis database index.
        """
        # Store Redis connection info for workers
        self.redis_host = host
        self.redis_port = port
        self.redis_db = db

    def build(self, step_id: str, worker_index: int, worker_count: int) -> _RedisKVSinkPartition:
        """
        Build a stateless sink partition for each worker.

        This creates a `_RedisKVSinkPartition` for each worker in the dataflow. 
        Each worker will independently write key-value pairs to Redis.

        :param step_id: The ID of the current dataflow step.
        :param worker_index: The index of the current worker.
        :param worker_count: The total number of workers in the dataflow.
        :return: A `_RedisKVSinkPartition` object for writing key-value pairs to Redis.
        """
        return _RedisKVSinkPartition(self.redis_host, self.redis_port, self.redis_db)
    
