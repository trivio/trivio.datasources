def mysql_output_stream(stream, partition, url, params):
  from trivio.support import SQLOutputStream
  fields = params.load('attributes', params)
  fields = fields.copy()
  return SQLOutputStream(stream, params.url, fields, params.start, params.end)#, url

