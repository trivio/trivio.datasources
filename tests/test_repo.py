import os
from datetime import datetime
from unittest import TestCase

from nose.tools import eq_
from mock import patch

from triv.io.datasources import repo
from triv.io import datasources
from triv.io.task import task

from fake import FakeRule, FakeTask


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

    task.push(FakeTask())
    
    input_stream = datasources.input_stream_for(None, 0, 'repo://dir1/doc1.txt', None)
    eq_('Hi mom!', input_stream[0].next())
    task.pop()
    
