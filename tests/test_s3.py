from datetime import datetime
from unittest import TestCase

from nose.tools import eq_

from triv.io.datasources import s3
from triv.io import datasources

class TestS3Source(TestCase):
  def setUp(self):
    datasources.load()

  def test_segments_between(self):
    url = 's3://AKIAIOV23F6ZNL5YPRNA:8Gwz48zgzwoYIZv70V4uGDD6%2fdNtHdbFq4kLXGlR@trivio.test/compensation'
    
    source = datasources.source_for(url)
    assert isinstance(source, s3.S3Source)
    urls = source.segment_between(datetime(2011,5,31), datetime(2011,6,1))
    eq_(len(urls),1)
    assert urls[0].startswith('http://trivio.test.s3.amazonaws.com/compensation/dt%3D2011-05-31T00%3A00%3A00/compensation_final.csv?')
    
    input_stream = datasources.input_stream_for(urls[0])
    stream = input_stream(None, 0, urls[0], None)[0]
    count=0
    for rec in stream:
      count+=1

    eq_(count,201)
