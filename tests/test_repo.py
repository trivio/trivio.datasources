import os
from datetime import datetime
from unittest import TestCase

from nose.tools import eq_
from mock import patch

from triv.io.datasources import repo
from triv.io import datasources


class FakeRule(object):
  @property
  def job_path(self):
    return os.path.join(
      os.path.dirname(__file__),
      'data/repo'
    )
  
  def path(self,path):
    return os.path.join(
      self.job_path,
      path
    )
  
class FakeTask(FakeRule):
  """The parts of the interface that the RepoSource  cares about
  are identical for the Rule and Task. We'll subclass here to
  keep the code clear"""

  def path(self, path):
    return super(FakeTask,self).path(path)

class TestRepoSource(TestCase):

  def test_segments_between(self):
    # mimic disco's global "Task" object
    
    url = 'repo://'
    source = datasources.source_for(url)
    assert isinstance(source, repo.RepoSource)
    
    source.rule = FakeRule()
    
    # todo: scan url's during sourcing
    # create a scheme that defrences during work
    
    # note: source_segment is called in pipeline not the workers
    urls = source.segment_between(datetime(2011,5,31), datetime(2011,6,1))
    self.assertSequenceEqual(
      urls,
      [
        'repo://dir1/doc1.txt',
        'repo://dir1/doc2.txt',
        'repo://dir2/doc1.txt',
      ]
    )
    
  def test_input_stream(self):
    repo.RepoSource.input_stream.func_globals['Task'] = FakeTask()
    
    input_stream = datasources.input_stream_for(None, 0, 'repo://dir1/doc1.txt', None)
    eq_('Hi mom!', input_stream.next())

    
