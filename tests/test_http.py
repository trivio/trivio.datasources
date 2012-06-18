from datetime import datetime
from unittest import TestCase

from nose.tools import eq_

from triv.io import datasources
from triv.io.datasources.http import HTTPSource

#TODO: find away to not expose params
from disco.worker.classic.worker import Params 

class TestHTTPSource(TestCase):
  def setUp(self):
    datasources.load()

  def test_http(self):
    url = 'http://google.com/'
    
    source = datasources.source_for(url)
    assert isinstance(source, HTTPSource)
    urls = source.segment_between(datetime(2011,5,31), datetime(2011,6,1))
    eq_(len(urls),1)
  
    params = Params()
    input_stream = datasources.input_stream_for(None, None, urls[0], params)
  