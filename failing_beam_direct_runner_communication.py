import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import threading

import logging
import sys
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


class WriteToCallback(beam.PTransform):
  def __init__(self, callback, lock):
    self._callback = callback
    self._lock = lock

  def expand(self, pcoll):
    return pcoll | beam.io.iobase.Write(_CallbackSink(self._callback, self._lock))


class _CallbackSink(beam.io.iobase.Sink):
  def __init__(self, callback, lock):
    self._callback = callback
    self._lock = lock

  def initialize_write(self):
    pass

  def open_writer(self, init_result, uid):
    return _CallbackWriter(self._callback, self._lock)

  def finalize_write(self, init_result, writer_results):
    pass


class _CallbackWriter(beam.io.iobase.Writer):
  def __init__(self, callback, lock):
    self._callback = callback
    self._lock = lock
    self._working_data = []

  def write(self, record):
    self._working_data.append(record)

  def close(self):
    with self._lock:
      self._callback(self._working_data)


def make_dump_to_list(visible_list):
  def dump(internal_list):
    logging.info("Dumping %s" % internal_list)
    visible_list.extend(internal_list)
  return dump


input = [1, 2, 3]
visible_list = []
lock = threading.Lock()

p = beam.Pipeline(options=PipelineOptions())
data = p | 'CreateInput' >> beam.Create(input)
data | 'DumpToList' >> WriteToCallback(
    make_dump_to_list(visible_list), lock)
result = p.run()
result.wait_until_finish()

logging.info("Pipeline finished.")
logging.info("Input: %s", input)
logging.info("Visible output: %s", visible_list)
assert input == visible_list
