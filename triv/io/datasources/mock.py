from datetime import datetime
from urlparse import urlparse, urlunparse, parse_qs

from dateutil.parser import parse as parse_date

from triv.io import datasources

class MockSource(datasources.DataSource):
  @staticmethod
  def input_stream(stream, size, url, params):
    '''Return an iterator'''
    return enumerate([['1','2','3']])
    
  @property
  def table(self):
    return self.parsed_url.netloc


  def earliest_record_time(self):
    dtstart = self.query.get('dtstart')
    if dtstart is None:
      self.dtstart = datetime.utcnow()
    else:
      self.dtstart = parse_date(dtstart[0])
    
    return self.dtstart
    
    
  def sample(self, start=None):
    """
    Returns an input_stream and url.
    
    Since the output of a mock is meant mostly for testing components, 
    we return the same values for the "sample"
    
    """
    scheme, netloc, path, params, query, fragment = self.parsed_url
    if start is not None:
      fragment = start.isoformat()
    else:
      fragment = ''
    
    return self.input_stream, [urlunparse((scheme, netloc, path, params, query, fragment))]

    
  def segment_between(self, start, end):
    '''Return a list of url's that belong in the given time range. Note all
    information needed to access a url must be encoded into the url'''
    
    reader, urls = self.sample(start)
    return urls
    
  
datasources.set_source_for_scheme(MockSource,'mock')
