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
    
# def write_csv():
#   # no colums, guess them
#   
#   headers = set(['$key'])
#   examined = []
# 
#   for i in xrange(1000):
#     try:
#       key, record = iter.next()
#     except StopIteration:
#       break
#     headers.update(record.keys())
#     record['$key'] = key
#     examined.append(record)
# 
#     
#   # sort and convert headers to a list
#   headers = sorted(headers)
#   
#   csvfile = StringIO.StringIO()
# 
#   def read_and_flush():
#     csvfile.seek(0)
#     data = csvfile.read()
#     csvfile.seek(0)
#     csvfile.truncate()
#     return data
#   
#   csvwriter = csv.DictWriter(csvfile, headers)
#   csvwriter.writeheader()
#   csvwriter.writerows(examined)
#   del examined
#   
#   # output the previously examined records
#   yield read_and_flush()
# 
#   while True:
#     # output the remaing rows a 1,000 at a time
#     for i in xrange(1000):
#       try:
#         key, record = iter.next()
#       except StopIteration:
#         yield read_and_flush()
#         return
#         break
#       record['$key'] = key          
#       csvwriter.writerow(record)
#       
#     yield read_and_flush()
# 
# 
# 
# 
