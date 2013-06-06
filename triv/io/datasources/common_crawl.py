import datetime

from triv.io import datasources
from triv.io.datasources.s3 import S3Source
# good for a day
seconds_good_for = 60*60*24

from disco.schemes.scheme_http import input_stream as http_input_stream

class CommonCrawlSource(S3Source):
  
  @staticmethod
  def input_stream(stream, size, url, params):

    params.content_type = 'application/x-arc-x'
    stream, size, url = http_input_stream(stream, size,url,params)
    return stream
  
  
  def _datetime_for_key(self, key):
    year,month,day = [int(i) for i in key.name.split('/')[2:5]]
    return datetime.datetime(year,month,day)
    
  def _prefix_for_datetime(self, dt):
      key_name = "{base}/{year}/{month:02d}/{day:02d}/".format(
        base=self.prefix,
        year = dt.year,
        month = dt.month,
        day = dt.day)
      return key_name
    
  def earliest_record_time(self):
    # Grab and parse the first key

    marker = self.prefix + '/'
    keys = self.bucket.get_all_keys(prefix=marker, marker=marker,  maxkeys=1)
    key = keys[0]

    #for key in self.bucket.list(self.prefix + '/'):
    return self._datetime_for_key(key)
  
  def sample(self):
    # TODO: be awesome if sample files were served from the file system
    start = self.earliest_record_time()
    marker = self._prefix_for_datetime(start)
    
    # grab the first key
    key = iter(self.bucket.list(marker = marker)).next()
    urls = [self.generate_url(key, force_http=True)]
    return self.input_stream, urls
    
    
  def segment_between(self, start,end):
    marker = self._prefix_for_datetime(start)
    urls = []
    
    limit = self.rule._params.get('maxinput', float('inf'))

    for key in self.bucket.list(prefix=self.prefix, marker=marker):
      dt = self._datetime_for_key(key)
      if dt is None:
        continue
        
      if dt > end:
        break
      else:
        limit -= 1
        if limit < 0:
          break
        if key.size > 0:
          urls.append(self.generate_url(key,force_http=True))
          
    return urls
    

datasources.set_source_for_url(CommonCrawlSource, 's3://aws-publicdatasets/common-crawl/crawl-002/')
datasources.set_source_for_url(CommonCrawlSource, 'http://aws-publicdatasets.s3.amazonaws.com/common-crawl/crawl-002/')

import calendar
class CommonCrawl2012Source(CommonCrawlSource):
  
  @property
  def segments(self):
    #Q: How long to source objects live?
    if not hasattr(self,'_segments'):
      key = self.bucket.lookup('common-crawl/parse-output/valid_segments.txt')
      self._segments = sorted([datetime.datetime.utcfromtimestamp(int(k)/1000.) for k in key.read().split('\n') if k])
    return self._segments
      
  def earliest_record_time(self):
    # Grab and parse the first key
    return self.segments[0]

  def sample(self):
    # TODO: be awesome if sample files were served from the file system
    start = self.earliest_record_time()
    prefix = self._prefix_for_datetime(start)

    # grab the first key
    key = iter(self.bucket.list(prefix = prefix)).next()
    urls = [self.generate_url(key, force_http=True)]

    return self.input_stream, urls

  def segment_between(self, start,end):
    limit = self.rule._params.get('maxinput', float('inf'))
    urls = []

    for segment in filter(lambda s: start <= s <= end, self.segments):    
      prefix = self._prefix_for_datetime(segment)

      for key in self.bucket.list(prefix=prefix):
        dt = self._datetime_for_key(key)
        urls.append(self.generate_url(key,force_http=True))
        limit -= 1
        if limit <= 0:

          return urls

    return urls
  
  
  def _datetime_for_key(self, key):
    """
    Rerturn the datetime of the key from the given URL.
    
    2012 Common crawl has the format of
    <prefix>/<unix timestamp in miliseconds>/<timestamp>_<sequence>.arc.gz
    
    For example, given a key that looks like this:
    common-crawl/parse-output/segment/1341690147253/1341708194364_11.arc.gz
    
    We pars out the <unix timestamp in miliseconds> in this case "1341690147253"
    and return it as a datetime object.
    
    """

    
    prefix, start_time, arc = key.name.rsplit('/',2)
    if arc[0].isdigit():
      time_stamp = float(start_time) / 1000.0
      return datetime.datetime.utcfromtimestamp(time_stamp)
    else:
      # it's metadata or textData 
      return None

  def _prefix_for_datetime(self, dt):
    """
    Given a datatime return the starting prefix
    """
    
    timestamp= int(
      (calendar.timegm(dt.timetuple()) * 1000) + 
      (dt.microsecond / 1000)
    )

    key_name = "{base}/{timestamp}/".format(
      base=self.prefix,
      timestamp = timestamp
    )

    return key_name


datasources.set_source_for_url(CommonCrawl2012Source, 's3://aws-publicdatasets/common-crawl/parse-output/segment/')
datasources.set_source_for_url(CommonCrawl2012Source, 'http://aws-publicdatasets.s3.amazonaws.com/common-crawl/parse-output/segment/')


class CommonCrawl2012MetadataSource(CommonCrawl2012Source):
  @staticmethod
  def input_stream(stream, size, url, params):
    params.content_type = 'application/x-hadoop-sequence'
    stream, size, url = http_input_stream(stream, size,url,params)
    return stream
  
  def _datetime_for_key(self, key):

    prefix, start_time, arc = key.name.rsplit('/',2)
    if arc.startswith('metadata'):
      time_stamp = float(start_time) / 1000.0
      return datetime.datetime.utcfromtimestamp(time_stamp)
    else:
      # it's metadata or textData 
      return None

  def _prefix_for_datetime(self, dt):
    """
    Given a datatime return the starting prefix
    """

    prefix = super(CommonCrawl2012MetadataSource, self)._prefix_for_datetime(dt)
    return prefix + 'metadata-'
    


datasources.set_source_for_url(CommonCrawl2012MetadataSource, 's3://aws-publicdatasets/common-crawl/parse-output/segment\?metadata')
datasources.set_source_for_url(CommonCrawl2012MetadataSource, 'http://aws-publicdatasets.s3.amazonaws.com/common-crawl/parse-output/segment/.*/metadata')
  