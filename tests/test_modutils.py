import os
from triv.io import modutils
from unittest import TestCase

class TestModUtils(TestCase):
  def setUp(self):
    self.path = os.path.join(os.path.dirname(__file__), 'data/fake_python_package')
    
  def test_modules_from_path(self):
    modules = list(modutils.modules_from_path(self.path))
    self.assertSequenceEqual(
      modules,
      ['a','b','subpackage.c']
    )
  