from urlparse import urlparse, parse_qs
from triv.io import datasources

class MockSource(object):
  def __init__(self, parsed_url):
    self.parsed_url = parsed_url
  
  @staticmethod
  def input_stream(stream, size, url, params):
    '''Return an iterator'''
    # parse the query flatten key's with single values
    record = parse_qs(urlparse(url).query)
    for key,val in record.items():
      if len(val) == 1:
        record[key] = val[0]
    
    return  iter([record])
    
  def segment_between(self, start, end):
    '''Return a list of url's that belong in the given time range. Note all
    information needed to access a url must be encoded into the url'''
    return [self.parsed_url.geturl()]
  
datasources.set_source_for_scheme(MockSource,'mock')
