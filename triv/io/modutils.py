import os
import re

# matches files ending with exactly '.py' and not starting with '__'
pattern = re.compile('^(?!__)(.*)\.py$')

def modules_from_path(path):
  """
  Recurse the given path returning the module names suited for importing
  """

  prefix_len = len(path) + 1
  for root, dirs, files in os.walk(path):
    for d in dirs[:]:
      if d.startswith('.') or d.startswith('test'):
        dirs.remove(d)
    
    if root != path and  '__init__.py' not in files:
      # not a package
      continue
      
    # conver path to file name dropping the appropriate '/' 
    # /blah/foo/baz/ -> filter(None,('','blah','baz', '')) -> 
    # '.'.join(['blah','foo','baz']) -> 'blah.foo.baz'

    # "/blah/foo/" -> ["", "blah", "foo", "" ]
    package = root[prefix_len:].split('/')
    for file in files:
      if file.startswith('test'):
        continue
        
      match = pattern.match(file)
      if match:
        name = match.group(1)
        yield ".".join(filter(None, package + [name]))
  

  