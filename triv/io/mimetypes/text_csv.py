from triv.io import datasources
import cStringIO as StringIO
import csv

@datasources.write_mimetype('text/csv')
class CSVOutputStream(object):
  def __init__(self, stream):
    self.samples = []
    self.sample_count = 1000
    self.headers = set()
    self.stream = stream

    
  def add(self, k,v):
    if isinstance(v, dict):
      record = v
    elif type(v) in (list, tuple):
      record = dict(enumerate(v))
    else:
      record = {0: v}
    record['$key'] = k
    if self.sample_count > 0:
      self.headers.update(record.keys())
      self.samples.append(record)
    
      if self.sample_count == 1:
        self.flush()
      self.sample_count -= 1
    else:
      self.csvfile.writerow(record)

  def flush(self):
    self.csvfile = csv.DictWriter(self.stream, sorted(self.headers))
    self.csvfile.writeheader()
    self.csvfile.writerows(self.samples)
    del self.samples[:]
    
    
  def close(self):
    if self.sample_count > 1:
      self.flush()
    self.stream.close()
    