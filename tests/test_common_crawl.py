import logging
from datetime import datetime, timedelta
from unittest import TestCase
from functools import partial

from nose.tools import eq_
from mock import patch

from . import MockS3Connection


MockS3Connection = partial(
  MockS3Connection, 
  mock_s3_fs={
    'aws-publicdatasets': {
      'keys': {
        'common-crawl/crawl-002/2009/09/17/0/1253228619531_0.arc.gz': 'doc 1',
        'common-crawl/crawl-002/2009/09/17/0/1253228619531_1.arc.gz': 'doc 1',
        'common-crawl/crawl-002/2009/09/17/0/1253228619531_2.arc.gz': 'doc 1',
        'common-crawl/crawl-002/2009/10/01/0/1253228619531_0.arc.gz': 'doc 1',
      }
    }
  }
)


from triv.io import datasources
from disco.worker.classic.worker import Params 



class TestCommonCrawlSource(TestCase):
  
  
  @patch('boto.connect_s3', MockS3Connection)
  def test_sourcing_common_crawl(self):
    # shut boto's debug messaging up during tests
    logging.getLogger('boto').setLevel(logging.INFO)
    

    datasources.load()
    url = 's3://bogus-key:bogus-secrect@aws-publicdatasets/common-crawl/crawl-002/'
    source = datasources.source_for(url)
    assert isinstance(source, datasources.common_crawl.CommonCrawlSource)
    start = datetime(2009,9,17, 00)
    eq_(source.earliest_record_time(), start)
    end = start + timedelta(days=1)
    
    urls = source.segment_between(start,end)
    # note the mockes3 connection doesn't bother to actually sign the urls
    self.assertSequenceEqual(
      urls,
      [
        'http://aws-publicdatasets.s3.amazonaws.com/common-crawl/crawl-002/2009/09/17/0/1253228619531_0.arc.gz?',
        'http://aws-publicdatasets.s3.amazonaws.com/common-crawl/crawl-002/2009/09/17/0/1253228619531_1.arc.gz?',
        'http://aws-publicdatasets.s3.amazonaws.com/common-crawl/crawl-002/2009/09/17/0/1253228619531_2.arc.gz?',
      ] 
    )
    
    # day's without data should return an empty list
    urls = source.segment_between(datetime(2009,9,21, 00), datetime(2009,9,22, 00))
    self.assertSequenceEqual(
      urls,
      []
    )
    
    urls = source.segment_between(datetime(2009,10,01, 00), datetime(2009,10,02, 00))
    self.assertSequenceEqual(
      urls,
      ['http://aws-publicdatasets.s3.amazonaws.com/common-crawl/crawl-002/2009/10/01/0/1253228619531_0.arc.gz?']
    )
    
  def __test_warc_mime_type(self):
    params = Params()
    input_stream = datasources.input_stream_for(None, None, segment[0], params)
    # TODO: figure out good way (that doesn't rack up amazon bills) to test the contents of the stream
    #for record in input_stream:
    #  print record['url']

