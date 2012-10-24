import os
from os.path import dirname, join

from unittest import TestCase

from nose import SkipTest
from nose.tools import eq_
from triv.io.mimetypes.application_x_arc import arc_input_stream

class TestARCReader(TestCase):
  def test_parse(self):
    sample_path = join(dirname(__file__), 'data/sample.arc.gz')
    if not os.path.exists(sample_path):
      # arc files are huge, download one from common crawl and place it
      # in th data directory
      raise SkipTest()
    stream = open(sample_path)

    # todo, inspect sample file by hand 
    #for record in a_input_stream(stream):
    #  print record