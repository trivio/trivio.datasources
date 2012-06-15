![Trivio Logo](http://www.triv.io/images/trivio_logo.png)
Data Sources
===


Overview
---
This repository provides the code to access external data sources that can be used in your triv.io projects. We want to  provide access to everything form personal databases such as MySQL, PostgreSQL, Oracle etc.. to public data sets like those available from CommonCrawl, Freebase, Wikipedia, Twitter or more. But well need your help.


You can help by  contributing the code needed to provide data from the sources we don't yet have access to. This document should help you get started.


How Data Sources Work
---

A data source is simply a python module that provides information about what [segments]() of data are currently available to triv.io along with the code to actually retrieve the data from it's source and conform it into the triv.io record format.

To get started let's walk through the creation of a dummy datasource. When we're done with this tutorial you can create a  triv.io script that looks like this.

```python
job = rule('mock://foo/')
```

1. To start, fork the [trivio.datasources repository](https://github.com/trivio/trivio.datasources)  from github. 

2. Then clone you're forked repository and change to the datasources director

```bash
$ git clone https://github.com/yourgituser/trivio.datasources.git
$ cd trivio.datasources/datasources
```

3. Create a new file named `mock.py` and add the following code to it.

```python
from urlparse import urlparse

def context(url):
	'''This function will be called once for each URL in a project. The
	results will be passed as the first argument to the other call back
	functions. Typically you would parse the URL here and extract information
	here to necessary to setup a connection to your data source. For example
	you might extract user name and password.'''
	context = urlparse(url).scheme

   
def segments_between(context, start, end):
	'''Return's a list of zero ore more URL's that belong in the segment 
	based on the time range passed in.'''
	
	return [{}]
     
def input_stream(stream, size, url, params):
   '''Returns an iterartor'''

   return [0] 
```

4. Test your code

5. Send a pull request to [github/trivio/trivio.datasources](https://github.com/trivio/trivio.datasources)

6. Once it's accepted you and everyone else will be able to use the module to import/export data from triv.io
    





