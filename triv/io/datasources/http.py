from datetime import datetime, timedelta
import disco.func

from triv.io import datasources

class HTTPSource(object):
  """Poll an http source"""
    
  def __init__(self, parsed_url):
    self.parsed_url = parsed_url
    
  @staticmethod
  def input_stream(stream, size, url, params):
    return disco.func.map_input_stream(stream, size,url,params)
    
  def earliest_record_time(self):
    return datetime.utcnow()
    
  def segment_between(self, start, end):
    # TODO: do an http head and use that for the datetime
    return [self.parsed_url + '#' + start.isoformat()]

datasources.set_source_for_scheme(HTTPSource,'http')
datasources.set_source_for_scheme(HTTPSource,'https')