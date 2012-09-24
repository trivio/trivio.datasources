import threading
class TaskProxy(object):
  """
  Provides an importable global context for passing the Task around
  """
  
  local = threading.local()
  
  @property
  def stack(self):
    if not hasattr(self.local, 'stack'):
      self.local.stack = []
    return self.local.stack

  def push(self, task):
    self.stack.append(task)

    
  def pop(self):
    self.stack.pop()
    
  def __getattr__(self, attr):
    if not len(self.stack):
      raise RuntimeError('working outside of a task context')
    task = self.stack[0]
    return getattr(task, attr)

  def __setattr__(self, attr, value):
    if not len(self.stack):
      raise RuntimeError('working outside of a task context')
    task = self.stack[0]
    return setattr(task, attr, value)

task = TaskProxy()