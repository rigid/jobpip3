
jobpip3 - information processing pipe framework



# description

Information (in form of Record() objects) is passed down a Pipe that is
formed by one or more Element() objects that are chained together.

Available pipe elements:
- Source(): an element with no input. Generates records.
- Function(): an element with input and output. Filters or alters records.
- Sink(): an element with no output. Stores/displays records.

A Record() is basically a dict-wrapper with additional checks and serialization.


## topography examples

simple pipe:
[SOURCE] -> [FUNCTION A] -> [FUNCTION B] -> [SINK]


pipe with one parallelized function (parallel=3):
            [FUNCTION A]            
[SOURCE] -> [FUNCTION A] -> [FUNCTION B] -> [SINK]
            [FUNCTION A]



## usage examples
tbd

