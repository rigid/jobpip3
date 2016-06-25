
import json



class Record(object):

    # file suffix for base record class output
    FILE_SUFFIX = "json"


    def __init__(self, *args, **kwargs):
        """:param record: record string, dict or Record()
                object that will be reconstructed
           :param missing_key_is_fatal: if not None, MissingAttributeError()
                  will be raised if record is missing a class-specifc key"""

        self._dict = {}

        # we can pass a record as keyword arg to use as stencil
        if "record" in kwargs:
            r = kwargs['record']
        # the first positional argument can be a record
        elif len(args) > 0:
            r = args[0]
        else:
            r = None

        # got a record passed as argument?
        if r is not None:
            # record can be a dict...
            if isinstance(r, dict):
                self._dict = r
            # string
            elif isinstance(r, (str, unicode)):
                self.parse(r)
            # ...or another Record() object
            elif isinstance(r, Record): self._dict = r._dict
            # ?
            else: raise ValueError(
                "unknown type: {}, Expected Record(), dict or string".format(
                    type(r)
                )
            )

        # store our classname inside Record(), so we can pass it to subprocess/
        # remote running Element()s
        self._dict['__classname__'] = self.__class__.__name__




    def _init_class_specific_keys(self, keys, **kwargs):

        ## handle class specific keys
        for key, native_type in keys:

            # if [<key>] is not set, yet...
            if key not in self._dict:
                # raise exception ?
                if kwargs.get('missing_key_is_fatal', None) is not None:
                    raise MissingAttributeError(key)
                # ...set it to None ?
                else:
                    self._dict[key] = None


            # if set already...
            else:
                # convert it to our type
                self._dict[key] = native_type(self._dict[key])

            # elements that are passed as argument to the constructor, will
            # override those of the instance passed as "record="
            if key in kwargs:
                self._dict[key] = native_type(kwargs._dict[key])


    def __str__(self):
        return self.dump()


    def __iter__(self):
        return self._dict.iterkeys()


    def __delitem__(self, key):
        del self._dict[key]


    def __getitem__(self, key):
        return self._dict[key]


    def __setitem__(self, key, val):
        self._dict[key] = val


    def __hash__(self):
        """return hash of this record"""

        def _hash(val):
            """return hash of an object"""
            h = 0
            if isinstance(val, (set, tuple, list)):
                for i in val:
                    h += _hash(i)
                return h

            elif isinstance(val, dict):
                for k,v in val.items():
                    h += _hash(k) + _hash(v)
                return h
            else:
                return hash(val)


        self.serialize()
        h = _hash(self._dict)
        self.unserialize()
        return h


    def serialize(self):
        """convert all fields of a record to serializable data"""
        pass


    def unserialize(self):
        """convert all serializable data of a record back to its original type"""
        pass


    def dump(self):
        """dump record as string"""
        # make this object serializable
        self.serialize()
        # dump to json string
        s = json.dumps(self._dict)
        # convert back to native format
        self.unserialize()
        return s


    def write(self, fd):
        # dump record
        fd.write(self.dump())
        # newline indicates end of record
        fd.write('\n')

    def parse(self, string):
        try: self._dict = json.loads(string)
        except ValueError as e:
            raise ValueError("{}\nCould not parse: {} ({})".format(
                e, string, len(string))
            )


    @classmethod
    def read(cls, fd):
        for line in iter(fd.readline, ""):
            yield cls(line)
