import urlparse
import os
import re

sources_by_url            = {}
sources_by_scheme         = {}

input_streams_for_urls    = {}
readers_by_mimetype       = {}

input_streams_for_domains   = {}
input_streams_for_schemes   = {}

class DataSource(object):
  
  #grain = "transaction|periodic snapshot|accumilating snapshot"
  #store_original = true|false
  
  # segment info
  #mime_type
  #record_count
  
  def __init__(self, parsed_url):
    self.parsed_url = parsed_url
    self.query = urlparse.parse_qs(self.parsed_url.query)


  # identify the group of segments
  # source_url
  
  # identify the individual segment
  # segment_url
  
  # identify a single url within the segment
  # urls
  
  # todo: depricated table/prefix and just use path
  @property
  def table(self):
    return self.parsed_url.path.strip('/')
  prefix = table 
  
  @property
  def path(self):
    return self.parsed_url.path
  
  @property
  def scheme(self):
    return self.parsed_url.scheme
    
  @property
  def url(self):
    """Returns the url for the source minus authentication"""
    parsed_url = self.parsed_url
    scheme, netloc, path, params, query, fragment = parsed_url
    netloc = ':'.join(filter(None, [parsed_url.hostname,parsed_url.port]))  
    url = urlparse.urlunparse((scheme, netloc, path, params, query, fragment))
    
    return url




def add_scheme(scheme):
  if scheme not in urlparse.uses_netloc:
    urlparse.uses_netloc.append(scheme)

  if scheme not in urlparse.uses_query:
    urlparse.uses_query.append(scheme)
  
def set_source_for_scheme(source, scheme):
  add_scheme(scheme)
  sources_by_scheme[scheme] = source
  
def set_source_for_url(source, url):
  sources_by_url[url] = source
  
def set_input_stream_for_scheme(scheme, input_stream):
  add_scheme(scheme)

  

def read_mimetype(mimetype):
  def wrap(f):
    readers_by_mimetype[mimetype] = f
  return wrap
    
def reader_for_mimetype(mimetype):
  return readers_by_mimetype.get(mimetype, lambda s:s)
  
def source_class_for(parsed_url):
  """Returns the source for the given url.
  
  """
  
  # strip out authentication

  scheme, netloc, path, params, query, fragment = parsed_url
  netloc = ':'.join(filter(None, [parsed_url.hostname,parsed_url.port]))  
  url = urlparse.urlunparse((scheme, netloc, path, params, query, fragment))

  src_cls = None
  
  # See if we have a source for this specific URL or subdomain
  for prefix, cls in sources_by_url.items():
    if url.startswith(prefix):
      return cls
  
      
  return sources_by_scheme.get(scheme)

def source_for(url):
  parsed_url = urlparse.urlparse(url)
  src_cls = source_class_for(parsed_url) 
  if src_cls:
    return src_cls(parsed_url)
    
def input_stream_for(stream, size, url, params):
  parsed_url = urlparse.urlparse(url)
  src_cls = source_class_for(parsed_url)

  if hasattr(src_cls, 'input_stream'):
    stream = src_cls.input_stream(stream, size, url, params)
    if hasattr(params, 'content_type'):
      stream = reader_for_mimetype(params.content_type)(stream)
      
    def log_if_error(self, stream):
      count = 0
      try:
        for record in stream:
          yield record
          count += 1
      except Exception, e:
        print "Error {} encountered at record {} in {} {}".format(
          e,
          count,
          stream,
          url
        )
      
    
    return log_if_error(stream)
  else:
    return None

def load():
  from ..mimetypes import application_json, application_x_arc
  for f in os.listdir(os.path.dirname(__file__)):
    match = re.match('^(?!__)(.*)\.py',f)
    if match:
      mod_name = match.group(1)
      module = __import__('triv.io.datasources.{0}'.format(mod_name), globals(),locals())
      



