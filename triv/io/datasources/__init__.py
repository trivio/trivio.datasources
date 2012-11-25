import urlparse
import os
import re
from pkg_resources import resource_listdir



sources_by_url            = []
sources_by_scheme         = {}

input_streams_for_urls    = {}
readers_by_mimetype       = {}
writers_by_mimetype       = {}

input_streams_for_domains   = {}
input_streams_for_schemes   = {}

from triv.io import task
from triv.io.modutils import modules_from_path

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
  
def set_source_for_url(source, url_pattern):
  if not url_pattern.endswith('*'):
    url_pattern += ".*"
  exp = re.compile(url_pattern)
  
  sources_by_url.append((url_pattern, exp, source))
  sources_by_url.sort(key=lambda t: t[0], reverse=True)
  
def set_input_stream_for_scheme(scheme, input_stream):
  add_scheme(scheme)



def source(scheme=None, url=None):
  """Decorator to register a datasoure for a given scheme, url or both
  
  Usage:
  To declare the source that handles a scheme
  
  @datastores.source(scheme='http')
  class HttpSource():
    ...
    
  To declare the source that handles a url

  @datastores.source(url='http://example.com/url')
  class HttpSpecificURLSource():
      ...
    
  """
  def wrap(f):
    if scheme:
      set_source_for_scheme(f,scheme)

    if url:
      set_source_for_url(f,url)

    return f

  return wrap


def read_mimetype(mimetype):
  """
  Decorator to register new mimetypes readers
  
  Usage:
  @datastores.read_mimetype
  def some_reader(stream):
    ...
  """
  def wrap(f):
    readers_by_mimetype[mimetype] = f
    return f
  return wrap
    
def reader_for_mimetype(mimetype):
  return readers_by_mimetype.get(mimetype, lambda s:s)


def write_mimetype(mimetype):
  """
  Decorator to register new mimetypes writers
  
  Usage:
  @datastores.read_mimetype
  def some_reader(stream):
    ...
  """
  
  def wrap(f):
    writers_by_mimetype[mimetype] = f
    return f
  return wrap


def writer_for_mimetype(stream, partition, url, params):
  if hasattr(params, 'mimetype'):
    cls = datasources.writers_by_mimetype[params.mimetype]
    return cls(stream)
  else:
    from disco.func import disco_output_stream
    return disco_output_stream(stream, partition, url, params)

  
def source_class_for(parsed_url):
  """Returns the source for the given url.
  
  """
  
  # strip out authentication

  scheme, netloc, path, params, query, fragment = parsed_url
  netloc = ':'.join(filter(None, [parsed_url.hostname,parsed_url.port]))  
  url = urlparse.urlunparse((scheme, netloc, path, params, query, fragment))

  src_cls = None
  
  # Search URLs from longest to shortest looking for the first one that shares
  # the common prefix
  #sources = sorted(sources_by_url.items(), key=lambda x: x[0], reverse=True)
  
  for url_pattern, exp, cls in sources_by_url:
    if exp.search(url):
      return cls
  
      
  return sources_by_scheme.get(scheme)

def source_for(url):

  parsed_url = urlparse.urlparse(url)
  src_cls = source_class_for(parsed_url) 
  if src_cls:
    return src_cls(parsed_url)


def error_proof(stream, size, url, params):
  """
  Lgos and continues reading even if an error was encountered
  """
  try:
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
  finally:
    try:
      task.pop()
    except IndexError:
      pass


    
def input_stream_for(stream, size, url, params):

  parsed_url = urlparse.urlparse(url)
  src_cls = source_class_for(parsed_url)

  if hasattr(src_cls, 'input_stream'):
    return src_cls.input_stream(stream, size, url, params)
  else:
    return None

def map_input_stream(stream, size, url, params):
  """
  Looks up an input stream if one is registered, if not
  falls back to disco's defaults.
  
  """
  
  # achtung! warning! when this function is called by the disco
  # node the globals in this module will no longer be visible
  # hence why we access everything through datasources
  from disco.util import schemesplit
  import disco.func
  from triv.io import datasources, task


  # Note: Task is a global set by disco, but not necsarrily seen by other object, 
  # we push it onto the context stack which will allow it to be imported by our
  # modules that need it

  try:
    task.push(Task)
  except NameError:
    # it's a test
    pass
  input_stream = datasources.input_stream_for(stream, size, url, params)

  if not input_stream:
    # we don't handle the given url, see if vanilla disco moduels can...
    
    try:
      # this is normally cleared when we're done iterating
      
      task.pop()
    except IndexError:
       pass
       
    input_stream = disco.func.map_input_stream(stream,size,url,params)

  # same code in classic/worker...
  if isinstance(input_stream, tuple):
    if len(input_stream) == 3:
      input_stream, size, url = input_stream
    else:
      input_stream, url = input_sttream

  if hasattr(params, 'content_type'):
    input_stream = datasources.reader_for_mimetype(params.content_type)(input_stream,size,url,params)


  return input_stream
  

def sample_input_stream(fd, url, size, params):
  count = 0
     
  for record in fd:
    if count == 100000:
      return
    else:
      count +=1

    yield record

def load(*additional_paths):
  import triv.io.mimetypes

  for f in resource_listdir(__name__,''):
    match = re.match('^(?!__)(.*)\.py',f)
    if match:
      mod_name = match.group(1)
      module = __import__('triv.io.datasources.{0}'.format(mod_name), globals(),locals())
  
  for path in additional_paths:
    for mod_name in modules_from_path(path):
      try:
        __import__(mod_name, globals(), locals())
      except ImportError,e:
        print e
        print "Error importing: {}".format(mod_name)
