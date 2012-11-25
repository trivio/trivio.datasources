import os
from os.path import dirname, join
from urlparse import urlparse


import json
import time
from unittest import TestCase

from nose import SkipTest
from nose.tools import eq_
from triv.io.mimetypes.application_x_hadoop_sequence import hadoop_input_stream


class TestHadoopReader(TestCase):
  def test_parse(self):
    sample_path = join(dirname(__file__), 'data/sample_metadata')
    if not os.path.exists(sample_path):
      # arc files are huge, download one from common crawl and place it
      # in th data directory
      raise SkipTest()
    stream = open(sample_path)

    # todo, inspect sample file by hand 


    counts = {}
    for url, metadata in hadoop_input_stream(stream,None,None,None):
      host = urlparse(str(url)).netloc
      key = '.'.join(reversed(host.split('.')))
      
      counts[key] = counts.get(key,0) +1

    for key, count in sorted(counts.items(), key=lambda t: t[1]):
      print key, count
    print "Total: {}".format(len(counts))
      
