import datetime

from disco.ddfs import DDFS


from triv.io import datasources

try:
  # only available under the scheduler
  import pipeline
except ImportError:
  pass

class FeverSource(datasources.DataSource):
  scheme       = "fever"
      
  @staticmethod
  def input_stream(stream, size, url, params):
    return disco.func.map_input_stream(stream, size,url,params)
  
  
  def __init__(self, parsed_url):
    super(FeverSource,self).__init__(parsed_url)
    self.ddfs  = DDFS(pipeline.app.config['DISCO_MASTER'])
    #self.ddfs = DDFS(self.disco_master)
    #self.table = source_url.split(':',1)[0]
    #self.prefix = self.table
    
  @property
  def table_url(self):
    """Returns base_url and table as base_url:table if base_url has been set otherwise just table"""
    return ':'.join(filter(None, (self.rule.base_url, self.table))) 

  def earliest_record_time(self):
    sources_rule = self.rule.rule_set.find_rule_by_target(self.rule.source.table)
    sources_source = sources_rule.source
    # TODO: Consider caching earliest_record_time so that dependent rules
    # don't cause more than one network query
    return sources_source.earliest_record_time()
    

  def sample(self):    
    prefix = "{}:{}".format(self.rule.base_url, self.path)
    return None, [self.ddfs.list(prefix)[0]]
    
  def segment_between(self, start, end):
    return None


datasources.set_source_for_scheme(FeverSource,'')