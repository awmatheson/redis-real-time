import os
import logging
from datetime import timedelta, datetime, timezone

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.connectors.demo import RandomMetricSource
from . import RedisStreamSink

HOST = os.getenv("REDIS_HOST", "localhost")
PORT = 6379
DATABASE = 0

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


flow = Dataflow("metrics_input")
metric1 = op.input("inp_v", flow, RandomMetricSource("v_metric", interval = timedelta(milliseconds=10)))
metric2 = op.input("inp_hz", flow, RandomMetricSource("hz_metric", interval = timedelta(milliseconds=10)))
metrics = op.merge("merge", metric1, metric2)
metrics = op.map("reformat", metrics, lambda x: {"metric":x[0], "value":x[1]})

op.inspect("input", metrics)
op.output("data_stream", metrics, RedisStreamSink(stream_name="metrics", host=HOST))
