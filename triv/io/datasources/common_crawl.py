import datetime

from triv.io import datasources
from triv.io.datasources.s3 import S3Source
# good for a day
seconds_good_for = 60*60*24

class CommonCrawlSource(S3Source):
  
  def __init__(self, parsed_url):
    super(CommonCrawlSource,self).__init__(parsed_url)
  
  def datetime_for_key(self, key):
    year,month,day = [int(i) for i in key.name.split('/')[2:5]]
    return datetime.datetime(year,month,day)
    
  def key_for_datetime(self, dt):
      key_name = "{base}/{year}/{month:02d}/{day:02d}/".format(
        base=self.prefix,
        year = dt.year,
        month = dt.month,
        day = dt.day)
      return key_name
    
  def earliest_record_time(self):
    # Grab and parse the first key

    for key in self.bucket.list(self.prefix + '/'):
      return self.datetime_for_key(key)
  
  def segment_between(self, start,end):

    prefix = self.key_for_datetime(start)
    urls = []
    for key in self.bucket.list(prefix=prefix):
      dt = self.datetime_for_key(key)
      if dt > end:
        break
      else:
        if key.size > 0:
          print key.metadata
          urls.append(key.generate_url(seconds_good_for,force_http=True))
        
    return urls
    

datasources.set_source_for_url(CommonCrawlSource, 's3://aws-publicdatasets/common-crawl/crawl-002/')
