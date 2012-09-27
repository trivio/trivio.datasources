import os
from datetime import datetime
from urlparse import urlparse, urlunparse, parse_qs

from dateutil.parser import parse as parse_date

from triv.io import datasources, task

class RepoSource(datasources.DataSource):
  """Sources files relative to the currently running job"""

  @staticmethod
  def input_stream(fd, size, url, params):
    """Maps the repo url "repo://<somepath>" to a file local to node and opens it    
    Opens the url locally on the node."""
    
    from disco.comm import open_url
    
    # repo://foo/blah -> foo/blah
    url = urlparse(url)
    path = url.netloc + url.path
    
    while path.startswith('/'):
      path = path[1:]
      
    path = os.path.join(task.disco_data, task.jobpath, path)
    #print path
    
    #url = 'file://' + path

    return (open(path),) #open_url(url)
  
  def earliest_record_time(self):
    return datetime.utcnow()
    
    
  def segment_between(self, start, end):
    '''Returns a segment whose urls are all files fonud with the job_path.
    
    Note that the scheme repo:// is retained and all files are relative to this.
    
    The input_stream, used in the worker, locate these files relative to the worker's job
    directory.
  
    '''

    prefix_len = len(self.rule.job_path)
    path = self.rule.path(self.parsed_url.path)
    
    urls = []
    for root, dirs, files in os.walk(path):
      for i,d in enumerate(dirs[:]):
        if d.startswith('.'):
          del dirs[i]
        
      dir = root[prefix_len:]
      
      for file in files:
        url = 'repo:/' + os.path.join(dir,file)
        urls.append(url)
    return urls
  
datasources.set_source_for_scheme(RepoSource,'repo')
