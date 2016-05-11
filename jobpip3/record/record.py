
import json



class Record(object):


    def __init__(self, *args):

        self._dict = {}

        if len(args) == 0:
            return

        if isinstance(args[0], dict):
            self._dict = args[0]

        elif isinstance(args[0], (str, unicode)):
            self.parse(args[0])


    def __iter__(self):
        return self._dict.iterkeys()


    def __getitem__(self, i):
        return self._dict[i]


    def __setitem__(self, i, v):
        self._dict[i] = v


    def dump(self):
        return json.dumps(self._dict)


    def write(self, fd):
        # dump record
        fd.write(self.dump())
        # newline indicates end of record
        fd.write('\n')


    def parse(self, string):
        try: self._dict = json.loads(string)
        except ValueError as e:
            raise ValueError("{0}\nCould not parse: {1}".format(e, string))


    @classmethod
    def read(cls, fd):
        for line in iter(fd.readline, ""):
            yield cls(line)
