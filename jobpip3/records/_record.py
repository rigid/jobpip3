
import json


class Record(object):

    # file suffix for base record class output
    FILE_SUFFIX = "json"


    def __init__(self, record=None, **kwargs):
        """:param record: record string, dict or Record()
                object that will be reconstructed
           :param missing_key_is_fatal: if not None, MissingAttributeError()
                  will be raised if record is missing a class-specifc key"""

        self._dict = {}

        # got a record passed as argument?
        if record is not None:
            # parse json string to dict
            if isinstance(record, (str, unicode)):
                record = json.loads(record)
            # process dict or Record() object...
            if isinstance(record, (Record, dict)):
                for k,v in record.iteritems(): self[k] = v
            # ?
            else: raise ValueError(
                "unknown type: {}, Expected Record(), dict or string".format(
                    type(record)
                )
            )


    def __repr__(self):
        return self.dump()


    def __str__(self):
        return self.dump()


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


        h = _hash(self.serialized())
        return h


    def __iter__(self):
        return self._dict.iterkeys()


    def __delitem__(self, key):
        del self._dict[key]


    def __getitem__(self, key):
        return self._dict[key]


    def __setitem__(self, key, val):
        # convert to native value if necessary
        self._dict[key] = Record._unserialize(key, val)

    def keys(self):
        return self._dict.keys()


    def iterkeys(self):
        return self._dict.iterkeys()


    def iteritems(self):
        return self._dict.iteritems()


    @staticmethod
    def _serialize(key, value):
        """convert field to serializable data"""
	# ...
        return value


    @staticmethod
    def _unserialize(key, value):
        """convert serializable value back to its original type"""
        # ...
        return value


    def serialized(self):
        """create serialized dict from record"""
        r = {}
        for k,v in self._dict.iteritems():
            r[k] = Record._serialize(k, v)
        return r


    def merge(self, record):
        """merge this record with another record"""
        self._dict.update(record._dict)


    def dump(self):
        """dump record as string"""
        # dump to json string
        s = json.dumps(self.serialized())
        return s


    def write(self, fd):
        # dump record
        fd.write(self.dump())
        # newline indicates end of record
        fd.write('\n')


    @staticmethod
    def read(fd):
        for line in iter(fd.readline, ""):
            yield Record(line)

