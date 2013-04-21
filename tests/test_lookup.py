import os
from os.path import dirname, join

from unittest import TestCase

from nose import SkipTest
from nose.tools import eq_
from triv.io import datasources


class Params(object):
  content_type= ""
  

class TestDataStoreRegistry(TestCase):
  
  def test_no_match(self):
    def test_look_order(self):
      """
      Ensure input_stream_for returns None if a url/mimetype combo
      that hasn't been registered is used.
      """
      
      url = "bogus-scheme://example.com/foo"
      params = Params()
      params.content_type = "application/not-registered"
      
      stream = datasources.input_stream_for(None, None, url, params)

      eq_(stream, None)


  def test_custom_mimereader_default_scheme(self):
    """Ensure lookup order of mimetypes
    
    The order:
    
    url specifies a  source
    source specifies the mimetype
    
    the rule can adapt the mimetype, does it ever need to overide the mimetype?
    
    
    """
    
    class FooStream(object):
      pass
        
    mimetype = 'application/x-foo'
    @datasources.read_mimetype(mimetype)
    def foo(stream, size, url, params):
      return FooStream()
    
    params = Params()
    params.content_type = mimetype

    url = "http://example.com/foo"

    stream = datasources.map_input_stream(None, None, url, params)

    assert isinstance(stream, FooStream)
    
