import datetime

from triv.io import datasources
from triv.io.datasources.s3 import S3Source
# good for a day
seconds_good_for = 60*60*24

from disco.schemes.scheme_http import input_stream as http_input_stream

class CommonCrawlSource(S3Source):
  
  @staticmethod
  def input_stream(stream, size, url, params):
    params.content_type = 'application/x-arc'
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

    for key in self.bucket.list(self.prefix + '/'):
      return self._datetime_for_key(key)
  
  def sample(self):
    # TODO: be awesome if sample files were served from the file system

    start = self.earliest_record_time()
    prefix = self._prefix_for_datetime(start)
    
    # grab the first key
    key = iter(self.bucket.list(prefix=prefix)).next()
    urls = [key.generate_url(seconds_good_for,force_http=True)]
    
    return self.input_stream, urls
    
    
  def segment_between(self, start,end):

    marker = self._prefix_for_datetime(start)
    urls = []
    
    limit = self.rule._params.get('maxinput', float('inf'))

    for key in self.bucket.list(prefix=self.prefix, marker=marker):
      try:
        dt = self._datetime_for_key(key)
      except:
        continue
        
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

import calendar
class CommonCrawlSource2012(CommonCrawlSource):
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
    time_stamp = float(start_time) / 1000

    return datetime.datetime.utcfromtimestamp(time_stamp)

  def _prefix_for_datetime(self, dt):
    """
    Given a datatime return the starting prefix
    """
    
    timestamp= int(calendar.timegm(dt.timetuple()) * 1000)

    key_name = "{base}/segment/{timestamp}".format(
      base=self.prefix,
      timestamp = timestamp
    )

    return key_name


datasources.set_source_for_url(CommonCrawlSource2012, 's3://aws-publicdatasets/common-crawl/parse-output/')
datasources.set_source_for_url(CommonCrawlSource2012, 'http://aws-publicdatasets.s3.amazonaws.com/common-crawl/parse-output/')
