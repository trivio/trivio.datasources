from triv.io import datasources
import json

@datasources.read_mimetype('application/json')
def input_stream(stream, size, url, params):
  yield json.loads(stream.read())

