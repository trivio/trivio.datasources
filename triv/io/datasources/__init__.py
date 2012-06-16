import urlparse
import os
import re

sources_by_url             = {}
sources_by_scheme          = {}

input_streams_for_urls      = {}
input_streams_for_mime_type = {}
input_streams_for_domains   = {}
input_streams_for_schemes   = {}


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
  
def source_for(url):
  """Returns the source for the given url.
  
  """
  
  # strip out authentication
  parsed_url = urlparse.urlparse(url)
  scheme, netloc, path, params, query, fragment = parsed_url
  netloc = ':'.join(filter(None, [parsed_url.hostname,parsed_url.port]))  
  url = urlparse.urlunparse((scheme, netloc, path, params, query, fragment))

  src_cls = None
  
  # See if we have a source for this specific URL or subdomain
  for prefix, cls in sources_by_url.items():
    if url.startswith(prefix):
      src_cls = cls
      break
  
  if src_cls is None:
    # look for a source by the scheme
    src_cls = sources_by_scheme.get(parsed_url.scheme)
  
  if src_cls:
    return src_cls(parsed_url)
    
def input_stream_for(url):
  parsed_url = urlparse.urlparse(url)
  src_cls = sources_by_scheme.get(parsed_url.scheme)
  if hasattr(src_cls, 'input_stream'):
    return src_cls.input_stream

def load():    
  for f in os.listdir(os.path.dirname(__file__)):
    match = re.match('^(?!__)(.*)\.py',f)
    if match:
      mod_name = match.group(1)
      module = __import__('triv.io.datasources.{0}'.format(mod_name), globals(),locals())  


