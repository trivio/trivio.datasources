from triv.io import datasources
import json
import sys

from hadoop.io import SequenceFile


@datasources.read_mimetype('application/x-hadoop-sequence')
def hadoop_input_stream(stream, size, url, params):
   
  stream.seek(0,2)
  size = stream.tell()
  stream.seek(0)
  reader = SequenceFile.Reader(stream, length=size)

  key_class = reader.getKeyClass()
  value_class = reader.getValueClass()

  key = key_class()
  value = value_class()

  while reader.next(key, value):
    yield key,value

  reader.close()

  