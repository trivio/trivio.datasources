from triv.io import datasources
import cStringIO as StringIO
import csv


@datasources.write_mimetype('text/csv')
class CSVOutputStream(object):
  def __init__(self, stream):
    pass
    
  def add(self, k,v):
    pass
    
  def close(self):
    pass
    
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
