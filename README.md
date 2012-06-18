![Trivio Logo](http://www.triv.io/images/trivio_logo.png)
Data Sources
===


Overview
---
This repository provides the code to access external data sources that can be used in your triv.io projects. We want to  provide access to everything from private databases such as MySQL, PostgreSQL, Oracle etc.. to public data sets like those available from CommonCrawl, Freebase, Wikipedia, Twitter or more. 

You can help by  contributing the code needed to provide data from the sources we don't yet have access to. This document should help you get started.


How Data Sources Work
---

A data source is simply a python module that provides information about what [segments](https://github.com/trivio/trivio.datasources/blob/master/docs/segments.md)
of data are currently available to triv.io along with the code to actually retrieve the data from
it's source and conform it into the triv.io record format.

Here is an example of how the 'mock' scheme was implemented for testing.

```python
from datetime import datetime
from urlparse import urlparse, urlunparse, parse_qs

from dateutil.parser import parse as parse_date

from triv.io import datasources

class MockSource(datasources.DataSource):
  @staticmethod
  def input_stream(stream, size, url, params):
    '''Return an iterator'''
    return enumerate([['1','2','3']]), None, url
    
    # parse the query flatten key's with single values
    record = parse_qs(urlparse(url).query)
    for key,val in record.items():
      if len(val) == 1:
        record[key] = val[0]
    
    return  iter([record])

  @property
  def table(self):
    return self.parsed_url.netloc

  def earliest_record_time(self):
    dtstart = self.query.get('dtstart')
    if dtstart is None:
      self.dtstart = datetime.utcnow()
    else:
      self.dtstart = parse_date(dtstart[0])
    
    return self.dtstart
  
    
  def segment_between(self, start, end):
    '''Return a list of url's that belong in the given time range. Note all
    information needed to access a url must be encoded into the url'''
    

    scheme, netloc, path, params, query, fragment = self.parsed_url
    fragment = start.isoformat()
    
    return [urlunparse((scheme, netloc, path, params, query, fragment))]
  
datasources.set_source_for_scheme(MockSource,'mock')
```

This source let's you create jobs whose source scheme are `mock` like this

```python

job = rule('mock://foo')

```




Contributing
----

1. To start, fork the [trivio.datasources repository](https://github.com/trivio/trivio.datasources)  from github. 

2. Then clone you're forked repository and change to the datasources director

```bash
$ git clone https://github.com/yourgituser/trivio.datasources.git
$ cd trivio.datasources/datasources
```

3. Create a new file named `mysource.py` and add your code to it

```python
from triv.io import datasources


class MySourceSource(datasource.DataSource):  
  @staticmethod  
  def input_stream(stream, size, url, params):
     '''Return an iterator of dictionaries here'''
    
  def segments_between(self, start, end):
     '''Return a list of url's that belong in the given time range. Note all
     information needed to access a url must be encoded into the url'''
     return [self.url.geturl()]
  
datasources.set_source_for_scheme('mock', MockSource)
```

4. Create a test for  your code see the test directory.  We use nose to run all our tests, if you don't have it run `pip install nose`

5. Update requirments.pip to include an additional dependencies. Please include version numbers

5. Send a pull request to [github/trivio/trivio.datasources](https://github.com/trivio/trivio.datasources)

6. Once it's accepted you and everyone else will be able to use the module to import/export data from triv.io
    





