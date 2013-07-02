import datetime

from disco.ddfs import DDFS
import disco.func


from triv.io import datasources


class FeverSource(datasources.DataSource):
  scheme       = "fever"
      
  @staticmethod
  def input_stream(stream, size, url, params):
    return disco.func.map_input_stream(stream, size,url,params)
      
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
    
    
  def segment_between(self, start, end):
    return None


datasources.set_source_for_scheme(FeverSource,'')