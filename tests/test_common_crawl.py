import logging
from datetime import datetime, timedelta
from unittest import TestCase

from nose.tools import eq_


from triv.io import datasources

class TestCommonCrawlSource(TestCase):
  def test(self):
    # shut boto's debug messaging up during tests
    logging.getLogger('boto').setLevel(logging.INFO)
    
    datasources.load()
    url = 's3://AKIAIOV23F6ZNL5YPRNA:8Gwz48zgzwoYIZv70V4uGDD6%2fdNtHdbFq4kLXGlR@aws-publicdatasets/common-crawl/crawl-002/'
    source = datasources.source_for(url)
    assert isinstance(source, datasources.common_crawl.CommonCrawlSource)
    start = datetime(2009,9,17, 00)
    assert  source.earliest_record_time() == start
    end = start + timedelta(days=1)
    segment = source.segment_between(start,end)

    pass