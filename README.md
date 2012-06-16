![Trivio Logo](http://www.triv.io/images/trivio_logo.png)
Data Sources
===


Overview
---
This repository provides the code to access external data sources that can be used in your triv.io projects. We want to  provide access to everything from private databases such as MySQL, PostgreSQL, Oracle etc.. to public data sets like those available from CommonCrawl, Freebase, Wikipedia, Twitter or more. 

You can help by  contributing the code needed to provide data from the sources we don't yet have access to. This document should help you get started.


How Data Sources Work
---

A data source is simply a python module that provides information about what [segments]() of data are currently available to triv.io along with the code to actually retrieve the data from it's source and conform it into the triv.io record format.

To get started we'll walk through creating a DataSource that can fetch segments from the `mock' scheme. When we're done with this tutorial you can create a  triv.io script that looks like this.

```
 job = rule('mock://foo?arg1=2&arg2=3')

 # The mock class will pass one record to the map function whose keys are
 # the arguments from the url
 @job.map
 def map(record, params):
   # mock://foo?arg1=2&arg2=3 -> (arg1,1), (arg2,1)
   for key in record.keys():
     yield key, 1
 
 @job.reduce(iter, params):
	 from disco.util import kvgroup
	    for word, counts in kvgroup(sorted(iter)):
	      yield word, sum(counts)

```

1. To start, fork the [trivio.datasources repository](https://github.com/trivio/trivio.datasources)  from github. 

2. Then clone you're forked repository and change to the datasources director

```bash
$ git clone https://github.com/yourgituser/trivio.datasources.git
$ cd trivio.datasources/datasources
```

3. Create a new file named `mock.py` and add the following code to it.

```python
from triv.io import datasources


class MockSource(object):
  def __init__(self, parsed_url):
    self.parsed_url = parsed_url
  
  @staticmethod  
  def input_stream(stream, size, url, params):
     '''Return an iterator'''
    
  def segments_between(self, start, end):
     '''Return a list of url's that belong in the given time range. Note all
     information needed to access a url must be encoded into the url'''
     return [self.url.geturl()]
  
triv.io.datasources.set_source_for_scheme('mock', MockSource)
triv.io.datasources.set_input_stream_for_scheme('mock', MockSource.input_stream) 


```

4. Test your code

5. Send a pull request to [github/trivio/trivio.datasources](https://github.com/trivio/trivio.datasources)

6. Once it's accepted you and everyone else will be able to use the module to import/export data from triv.io
    





