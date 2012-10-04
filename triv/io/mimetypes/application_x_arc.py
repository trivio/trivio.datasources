from triv.io import datasources
from triv.gzip import GzipFile
import warc

@datasources.read_mimetype('application/x-arc')
def input_stream(stream):
  arc = warc.ARCFile(fileobj=GzipFile(fileobj=stream))
  for doc in arc:
    record = dict(doc.header)
    record['payload'] = doc.payload.decode('ascii', 'ignore')
    yield record


