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


from collections import namedtuple



@datasources.read_mimetype('application/x-arc-x')
def arc_input_stream(stream):
  stream = GzipFile(fileobj=stream)
  
  file_header = stream.readline().rstrip()
  file_source = stream.readline().rstrip()
  column_desc = stream.readline().rstrip()
  column_desc = [f.lower().replace('-','_') for f in column_desc.split(' ')]
  column_desc.append('payload')
  #ARCRecord = namedtuple('ARCRecord', column_desc + ['payload'])
  
  #trailing newline
  stream.readline()
  
  while True:
    record = stream.readline()
    if record == '':
      break
    else:
      record = record.rstrip().split(' ')
      
    arc_length = record[-1] = int(record[-1])
    # add payload
    record.append(stream.read(arc_length))
  
    yield dict(zip(column_desc, record))
  
