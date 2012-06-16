from datetime import datetime
from dateutil import parser
import urlparse
import boto

from triv.io import datasources

class S3Source(object):
  """Treats S3 like a database"""
    
  def __init__(self, parsed_url):
    self.parsed_url = parsed_url
    self.acccess_key_id    = urlparse.unquote(parsed_url.username)
    self.secret_access_key = urlparse.unquote(parsed_url.password)

    self.conn = boto.connect_s3(self.acccess_key_id , self.secret_access_key)
    self.bucket_name = parsed_url.hostname
    self.prefix = parsed_url.path.strip('/')
    self.bucket = self.conn.get_bucket(self.bucket_name,validate=False)

    
  def earliest_record_time(self):
    # Grab and parse the first key

    for prefix in self.bucket.list(self.prefix + '/', delimiter='/'):
      params = dict([entry.split('=',1) for entry in prefix.split('/')])
      date = params['dt']
      return parser.parse(date)
    
    # if bucket is empty return now
    return datetime.utcnow()

  def segment_between(self, start, end):
    # TODO: this won't return a range of key's only the bucket that start's exactly
    # with the start time
    
    keys = self.bucket.list("{0}/dt={1}/".format(self.prefix, start.isoformat()), delimiter='/')
    seconds_good_for = 60*60*24
  
    urls = [k.generate_url(seconds_good_for,force_http=True) for k in keys if k.size > 0]
    return urls
    
datasources.set_source_for_scheme(S3Source,'s3')