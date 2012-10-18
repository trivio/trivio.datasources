import datetime

from triv.io import datasources
from triv.io.datasources.s3 import S3Source
# good for a day
seconds_good_for = 60*60*24

from disco.schemes.scheme_http import input_stream as http_input_stream

class CommonCrawlSource(S3Source):
  
  # todo sources should define the map_input_stream as a series
  # def arc_to_mime(doc, size, url, params):
  #  return mime_stream(doc.content_type)
  
  # of input_streams [http_input_stream, mime_stream('application/x-arc'), arc_to_mime ]
  
  
  
  @staticmethod
  def input_stream(stream, size, url, params):
    params.content_type = 'application/x-arc'
    stream, size, url = http_input_stream(stream, size,url,params)
    return stream
  
  
  def _datetime_for_key(self, key):
    year,month,day = [int(i) for i in key.name.split('/')[2:5]]
    return datetime.datetime(year,month,day)
    
  def _key_for_datetime(self, dt):
      key_name = "{base}/{year}/{month:02d}/{day:02d}/".format(
        base=self.prefix,
        year = dt.year,
        month = dt.month,
        day = dt.day)
      return key_name
    
  def earliest_record_time(self):
    # Grab and parse the first key

    for key in self.bucket.list(self.prefix + '/'):
      return self._datetime_for_key(key)
  
  def sample(self):
    # TODO: be awesome if sample files were served from the file system

    start = self.earliest_record_time()
    prefix = self._key_for_datetime(start)
    
    # grab the first key
    key = iter(self.bucket.list(prefix=prefix)).next()
    urls = [key.generate_url(seconds_good_for,force_http=True)]
    
    return self.input_stream, urls
    
    
  def segment_between(self, start,end):

    prefix = self._key_for_datetime(start)
    urls = []
    
    limit = self.rule._params.get('maxinput', float('inf'))
    
    for key in self.bucket.list(prefix=prefix):
      dt = self._datetime_for_key(key)
      if dt > end:
        break
      else:
        limit -= 1
        if limit < 0:
          break
        if key.size > 0:
          urls.append(key.generate_url(seconds_good_for,force_http=True))
          
        
    return urls
    

datasources.set_source_for_url(CommonCrawlSource, 's3://aws-publicdatasets/common-crawl/crawl-002/')
datasources.set_source_for_url(CommonCrawlSource, 'http://aws-publicdatasets.s3.amazonaws.com/common-crawl/crawl-002/')
