Packman
=======

Evaluation-orthogonal serialisation of Haskell data, as a library

### Haskell API

This package provides Haskell data serialisation independent of evaluation,
by accessing the Haskell heap using foreign primitive operations.
Any Haskell data structure (with a few limitations) can be serialised and
later deserialised in the same or a new run of the same program (that means,
the same executable file).

A Haskell API around the C-implemented core provides the following API:

```
trySerializeWith :: a -> Int -> IO (Serialized a) -- Int is maximum buffer size to use
trySerialize :: a -> IO (Serialized a)            -- uses default (maximum) buffer size
deserialize :: Serialized a -> IO a
```

Note that this serialisation is orthogonal to evaluation: the argument is
serialised **in its current state of evaluation**, it might be entirely
unevaluated (a thunk) or only partially evaluated (containing thunks).

The `Serialized a` type is an opaque representation of serialised Haskell data (it
contains a `ByteArray`). `Serialized a` provides instances for `Show` and `Read`
which satisfy `read . show == id`, and a `Binary` instance. For these instances,
types are checked dynamically type-safe, therefore the `Typeable` context.

### Advantages

The library enables sending and receiving data between different nodes of a distributed Haskell system. This is where the code originated: the [Eden runtime system](http://www.mathematik.uni-marburg.de/~eden/). You might want to read a related paper which describes parts of it ( [IFL 2006](http://www.mathematik.uni-marburg.de/~eden/paper/Eden-IFL06-webversion.pdf) ).

Apart from this obvious application, the functionality can be used to optimise programs by memoisation (across different program runs), and to checkpoint program execution in selected places. Both uses are exemplified in the slide set linked above.

### Drawbacks

As serialisation essentially provides a way to duplicate data (and therefore destroy sharing), certain data should not be serialisable. Most prominently, these include the mutable types `MVar`, `IORef`, and all STM-related types. However, the presence of those types is sometimes not apparent; they occur **within the thunk computing a value of different type**. The most annoying example is lazy file I/O: lazily reading a file entails holding a "half-closed" file handle (essentially an MVar), and will make serialisation fail for the read data.
The ugly solution in the library: the API signals such conditions as exceptions.

Another limitation is that serialised data **can only be used by the very same binary**. This is however common for many approaches to distributed programming using functional languages.

If you find this library useful, I would be happy to hear from you. Patches are welcome.

-----------------

#### Reading material

In brief, this is the packing code of Eden and GpH, ripped out of the runtime
system and rewritten to make it thread-safe and return exception codes when
it fails.

Most of what is provided by the library was already there when I
presented at HIW in 2013,
[see slides from HIW 2013](http://www.haskell.org/wikiupload/2/28/HIW2013PackingAPI.pdf)
but all code was in the runtime system then.

The basic idea was described earlier, in a
[2010 IFL paper](http://www.diku.dk/~berthold/papers/mainIFL10-withCopyright.pdf),
including a study of possible applications, especially checkpointing
and memoisation.

#### Acknowledgements

The idea to separate serialisation from other functionality of the parallel runtime system was suggested by Phil Trinder in 2009.  
Hans-Wolfgang Loidl introduced me to the GUM packing code, worked with me on the parallel runtime system for a long time, and always provided valuable feedback.
Kevin Hammond is the original author of the packing code used by packman and the Eden RTS. It has been rewritten a few times and improved by a number of people (including Phil Trinder and Hans-Wolfgang Loidl).  
Michael Budde and Åsbjørn Jøkladal assembled the first cabalised library version as a student project in our course "Topics in programming languages" 2014 (where the topic was parallel functional programming).
