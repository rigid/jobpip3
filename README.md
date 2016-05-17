
jobpip3 - information processing pipe framework



# description

Information (in form of Record() objects) is passed down a Pipe that is
formed by one or more Element() objects that are chained together.

A Record() is basically a dict-wrapper with additional checks and serialization.
An Element() basically wraps a python generator function that yields Record()s 
and optionally takes a Record() iterable as input.

Available pipe elements:
- Source(): an element with no input. Generates records.
- Function(): an element with input and output. Filters or alters records.
- Sink(): an element with no output. Stores/displays records.




## topography examples

simple pipe:
[SOURCE] -> [FUNCTION A] -> [FUNCTION B] -> [SINK]


pipe with one parallelized function (mode='subprocess', parallel_workers=3):
            [FUNCTION A]            
[SOURCE] -> [FUNCTION A] -> [FUNCTION B] -> [SINK]
            [FUNCTION A]



## usage examples

Simple example:

```python

    src = NumberSource()           # [{ 'i' : 1 }, { 'i' : 2 }, ... ]
    func1 = MultiplyFunction(2)    # [{ 'i' : 2 }, { 'i' : 4 }, ... ] 
    func2 = AddFunction(1)         # [{ 'i' : 3 }, { 'i' : 5 }, ... ]
    sink = CsvSink("foo.csv")      # writes csv
    
    # run pipe
    records = src.well()
    records = func1.process(records)
    records = func2.process(records)
    sink.drain(records)
```        

Another simple example:

```python
    
    src = NumberSource()           # [{ 'i' : 1 }, { 'i' : 2 }, ... ]
    sink = CsvSink("foo.csv")      # writes csv
    
    # run pipe
    records = src.well()
    
    # we could iterate the result of func2
    for record in records:
       # we could manipulate each record
       if record['i'] == 42: print "got answer"
       # save single record
       sink.drain(record)
```
