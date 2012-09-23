from datetime import datetime
from unittest import TestCase

from nose.tools import eq_
from mock import patch

from functools import partial

from disco.worker.classic.worker import Params 



from triv.io.datasources import s3
from triv.io import datasources


from . import MockS3Connection
MockS3Connection = partial(
  MockS3Connection, 
  mock_s3_fs={
    'trivio.test': {
      'keys': {
        'folder/dt=2011-05-31T00:00:00/doc1.csv': 'doc 1',
        'folder/dt=2011-05-31T00:00:00/doc2.csv': 'doc 2',
        'folder/dt=2011-06-01T00:00:00/doc3.csv': 'doc 3'
      }
    }
  }
)



class TestS3Source(TestCase):
  def setUp(self):
    datasources.load()

  @patch('boto.connect_s3', MockS3Connection)
  def test_segments_between(self):
    url = 's3://AKIAIOV23F6ZNL5YPRNA:8Gwz48zgzwoYIZv70V4uGDD6%2fdNtHdbFq4kLXGlR@trivio.test/folder'
    
    source = datasources.source_for(url)
    assert isinstance(source, s3.S3Source)
    urls = source.segment_between(datetime(2011,5,31), datetime(2011,6,1))
    eq_(len(urls),2)
    
    # note the mockes3 connection doesn't bother to actually sign the urls
    self.assertSequenceEqual(
      urls,
      [
        'http://trivio.test.s3.amazonaws.com/folder/dt%3D2011-05-31T00%3A00%3A00/doc1.csv?',
        'http://trivio.test.s3.amazonaws.com/folder/dt%3D2011-05-31T00%3A00%3A00/doc2.csv?',
      ]
    )
    
    
    urls = source.segment_between(datetime(2011,6,1), datetime(2011,6,2))
    
    # note the mockes3 connection doesn't bother to actually sign the urls
    self.assertSequenceEqual(
      urls,
      [
        'http://trivio.test.s3.amazonaws.com/folder/dt%3D2011-06-01T00%3A00%3A00/doc3.csv?',
      ] 
    )
    
    eq_(len(urls),1)

    
