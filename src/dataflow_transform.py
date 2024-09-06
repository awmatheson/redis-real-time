import os
import logging
from datetime import timedelta, datetime, timezone

import bytewax.operators as op
from bytewax.dataflow import Dataflow
import bytewax.operators.windowing as win
from bytewax.operators.windowing import EventClock, TumblingWindower
import numpy as np

from . import RedisStreamSource, RedisKVSink

HOST = os.getenv("REDIS_HOST", "localhost")
PORT = 6379
DATABASE = 0

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

flow = Dataflow("feature_transform")

inp = op.input("inp", flow,
               RedisStreamSource(
                   HOST,
                   PORT,
                   DATABASE,
                   "metrics"
               ))

def reformat(ts__metric):
    ts, metric = ts__metric
    return (metric[b'metric'].decode(), 
            {"ts": int(ts.decode().split("-")[0])/1000.0, 
             "value": float(metric[b'value'])})

metrics = op.map("rekey", inp, reformat)
op.inspect("metrics", metrics)

def get_event_time(reading):
    return datetime.fromtimestamp(reading["ts"], tz=timezone.utc)

cc = EventClock(get_event_time, wait_for_system_duration=timedelta(seconds=0.1))

# And a 5 seconds tumbling window
align_to = datetime(2024, 9, 1, tzinfo=timezone.utc)
wc = TumblingWindower(align_to=align_to, length=timedelta(seconds=1))

windowed_stream = win.collect_window("window", metrics, cc, wc)

op.inspect("input", windowed_stream.down)

def average(key__win_id__readings):
    key, (win_id, readings) = key__win_id__readings
    vals = [x['value'] for x in readings]
    return (f"{key}-{win_id}", str(float(np.average(vals))))

avg_stream = op.map("avg", windowed_stream.down, average)
op.inspect("avg_stream", avg_stream)
op.output("feature_out", avg_stream, RedisKVSink())
