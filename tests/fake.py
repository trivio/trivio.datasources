import os

class FakeRule(object):
  def __init__(self):
    self._params = {}
    
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

  jobpath = FakeRule.job_path
  def path(self, path):
    return super(FakeTask,self).path(path)
  
  @property
  def disco_data(self):
    return self.path('')
