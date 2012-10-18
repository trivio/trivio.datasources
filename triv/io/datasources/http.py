from datetime import datetime, timedelta
from disco.schemes.scheme_http import input_stream as http_input_stream

from triv.io import datasources

class HTTPSource(datasources.DataSource):
  """Poll an http source"""
        
  @staticmethod
  def input_stream(stream, size, url, params):
    stream, size, url = http_input_stream(stream, size,url,params)
    params.headers = stream.headers
    params.content_type = stream.headers['content-type']
    return stream
    
  def earliest_record_time(self):
    return datetime.utcnow()
    
  def segment_between(self, start, end):
    # TODO: do an http head and use that for the datetime
    return [self.parsed_url.geturl() + '#' + start.isoformat()]

datasources.set_source_for_scheme(HTTPSource,'http')
datasources.set_source_for_scheme(HTTPSource,'https')