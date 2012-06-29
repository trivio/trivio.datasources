from triv.io import datasources
from triv.gzip import GzipFile
import warc

@datasources.read_mimetype('application/x-arc')
def input_stream(stream):
  stream = warc.ARCFile(fileobj=GzipFile(fileobj=stream))
  count = 0
  for record in stream:
    yield record
    count += 1
    if count > 10: break
