import os
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

from fake import FakeRule


class TestCommonCrawlSource(TestCase):
  
  def s3_mock(self):
    data = open(os.path.dirname(__file__) + '/data/2012_common_crawl_listing.txt')
    keys = dict([(l.strip(), 'bogus data') for l in data])
    
    s3 = MockS3Connection()
    s3.mock_s3_fs['aws-publicdatasets']['keys'] = keys
    
    s3.mock_s3_fs['aws-publicdatasets']['keys']['common-crawl/parse-output/valid_segments.txt'] = (
      '1341690147253\n'
      '1341690150308\n'
      '1346981172155\n'
    )
    return s3
  
  
  @patch('boto.connect_s3', MockS3Connection)
  def test_sourcing_common_crawl_2010(self):
    # shut boto's debug messaging up during tests
    logging.getLogger('boto').setLevel(logging.INFO)
    

    datasources.load()
    url = 's3://bogus-key:bogus-secrect@aws-publicdatasets/common-crawl/crawl-002/'
    source = datasources.source_for(url)

    source.rule = FakeRule()
    assert isinstance(source, datasources.common_crawl.CommonCrawlSource)
    start = datetime(2009,9,17, 00)
    eq_(source.earliest_record_time(), start)
    end = start + timedelta(days=1)
    
    urls = source.segment_between(start,end)
    # note the mocks3 connection doesn't bother to actually sign the urls
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
    

  def test_sourcing_common_crawl_2012(self):
    # shut boto's debug messaging up during tests
    logging.getLogger('boto').setLevel(logging.INFO)
   
    with patch('boto.connect_s3', lambda a1,a2:self.s3_mock()):
      datasources.load()
      url = 's3://bogus-key:bogus-secrect@aws-publicdatasets/common-crawl/parse-output/segment/'
      source = datasources.source_for(url)

      source.rule = FakeRule()
      eq_(source.__class__, datasources.common_crawl.CommonCrawl2012Source)
      
      start = datetime(2012, 7, 7, 19, 42, 27, 253000)
      eq_(source.earliest_record_time(), start)
      end = start + timedelta(days=1)

      urls = source.segment_between(start,end)
      
      eq_(len(urls), 71)
      
      eq_(
        urls[0],
        'http://aws-publicdatasets.s3.amazonaws.com/common-crawl/parse-output/segment/1341690147253/1341708194364_11.arc.gz?'
      )
      
      eq_(
        urls[-1],
        'http://aws-publicdatasets.s3.amazonaws.com/common-crawl/parse-output/segment/1341690150308/1341690944267_36.arc.gz?'
      )
      
      
      # day's without data should return an empty list
      urls = source.segment_between(datetime(2009,9,21, 00), datetime(2009,9,22, 00))
      self.assertSequenceEqual(
        urls,
        []
      )

      urls = source.segment_between(datetime(2012,9,07, 00), datetime(2012,9,30, 23))      
      eq_(len(urls), 10)
      
      
      # we should see everything if we query for the whole year
      urls = source.segment_between(datetime(2012,1,01, 00), datetime(2012,12,31, 23))      
      eq_(len(urls), 81)

#
  def test_sourcing_common_crawl_2012_metadata(self):
    # shut boto's debug messaging up during tests
    logging.getLogger('boto').setLevel(logging.INFO)
 
    with patch('boto.connect_s3', lambda a1,a2:self.s3_mock()):
      datasources.load()
      url = 's3://bogus-key:bogus-secrect@aws-publicdatasets/common-crawl/parse-output/segment?metadata'
      source = datasources.source_for(url)

      source.rule = FakeRule()
      eq_(source.__class__, datasources.common_crawl.CommonCrawl2012MetadataSource)
      start = datetime(2012, 7, 7, 19, 42, 27, 253000)

      eq_(source.earliest_record_time(), start)
      end = start + timedelta(days=1)

      urls = source.segment_between(start,end)

      eq_(len(urls), 7)
      
      # day's without data should return an empty list
      urls = source.segment_between(datetime(2009,9,21, 00), datetime(2009,9,22, 00))
      self.assertSequenceEqual(
        urls,
        []
      )
      
  
    

    

