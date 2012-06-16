### Segments
Segments are a chunk of data that can be referenced by one or more URLs.

###Build phases
1. Source - Scans a data source for new data for a given time frame

2. Target - Given the new data available generates zero or more new target segments that will be built from the source data.

3. Repeat step 2 by constructing new targets based on the newly added targets and source until no new targets have been added.

5. Build - in this phase a target node that have not been built will be scheduled to build as soon as the data it is dependent upon is available. This is immediately for segments that only depend on data that was discovered during the sourcing phase (step 2). Segments that depend on another segment being built first will wait until their dependencies have been built.




given a url

s3://beoteheth/toehtuhenehu


We have the scheme which identifies the transport

The system should query the url and "guess" the mime-type. 

Given the scheme, mime-type and url the system should locate the 
input stream method that's is best suited to handling the given 
url.

In order of preference the system looks for a handler

1. input stream specified in the rule
1. Stream specified by the exact url
1. stream based on subdomain/subpath (review cookie rules)
1. stream based on mime-type
1. stream based on transport (for instance the default )
1. Exact URL 





We need to make an input which provides a iter based set of data
 


Contributing
---

1. Fork this project
1. Create a python file under datasources/ called `<data_source>.py` replacing *data_source* with an appropriate name for your data source.

    def segments_between(start, end):
        yield [… list of urls …]
    
    def input_stream(stream,url,size):
      pass
  
1. Send a pull request to [github/trivio/trivio.datasources](https://github.com/trivio/trivio.datasources)
2. Once it's accepted you and everyone else will be able to use the module to import/export data from triv.io





