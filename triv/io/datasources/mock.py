from datetime import datetime
from urlparse import urlparse, urlunparse, parse_qs

from dateutil.parser import parse as parse_date

from triv.io import datasources

class MockSource(datasources.DataSource):
  @staticmethod
  def input_stream(stream, size, url, params):
    '''Return an iterator'''
    return enumerate([['1','2','3']]), None, url
    
    # parse the query flatten key's with single values
    record = parse_qs(urlparse(url).query)
    for key,val in record.items():
      if len(val) == 1:
        record[key] = val[0]
    
    return  iter([record])

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
  
    
  def segment_between(self, start, end):
    '''Return a list of url's that belong in the given time range. Note all
    information needed to access a url must be encoded into the url'''
    

    scheme, netloc, path, params, query, fragment = self.parsed_url
    fragment = start.isoformat()
    
    return [urlunparse((scheme, netloc, path, params, query, fragment))]
  
datasources.set_source_for_scheme(MockSource,'mock')
