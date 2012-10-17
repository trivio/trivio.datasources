from triv.io import datasources
import json

@datasources.write_mimetype('application/x-json-stream')
class StreamingJsonOutputStream(object):
  def __init__(self, stream):
    self.stream = stream
    
  def add(self, k,v):
    self.stream.write(json.dumps((k,v)) + '\n')
        
  def close(self):
    self.stream.close()
