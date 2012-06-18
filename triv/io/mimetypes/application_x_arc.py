from triv.io import datasources
from triv.gzip import GzipFile
import warc

@datasources.read_mimetype('application/x-arc')
def input_stream(stream):
  stream = warc.ARCFile(fileobj=GzipFile(fileobj=stream))
  return stream