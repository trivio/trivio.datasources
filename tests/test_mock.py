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
    eq_(urls[0], url+'#2011-05-31T00:00:00')
  
    input_stream = datasources.input_stream_for(None, 0, urls[0], None)[0]
    record = iter(input_stream).next()
    self.assertSequenceEqual(record, (0, ['1', '2', '3']))
