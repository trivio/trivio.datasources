from datetime import datetime
from unittest import TestCase

from nose.tools import eq_

from triv.io.datasources import mock
from triv.io import datasources

class TestMockSource(TestCase):
  def test_segments_between(self):
    url = 'mock://foo?arg1=a&arg2=b'
    source = datasources.source_for(url)
    assert isinstance(source, mock.MockSource)
    urls = source.segment_between(datetime(2011,5,31), datetime(2011,6,1))
    eq_(len(urls),1)
    eq_(urls[0], url)
  
    input_stream = datasources.input_stream_for(urls[0])
    record = input_stream(None, 0, url, None).next()
    self.assertDictEqual(record, {'arg1':'a','arg2':'b'})
