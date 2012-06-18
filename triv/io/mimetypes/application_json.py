from triv.io import datasources
import json

@datasources.read_mimetype('application/json')
def input_stream(stream):
  yield json.loads(stream.read())

