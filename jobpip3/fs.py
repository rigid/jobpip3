"""filesystem utils"""

import os
import errno



def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as e:  # Python >2.5
        if e.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def dir(destpath, prefix=None, suffix=None):

    for filename in os.listdir(destpath):

        # respect prefix ?
        if prefix is not None:
            # skip files that don't start with prefix
            if not filename.startswith(prefix):
                continue

        # respect suffix ?
        if suffix is not None:
            # skip files that don't end with suffix of sink that is
            # used by the aggregator (e.g. "*.json")
            if not filename.endswith(suffix):
                continue

        yield filename


def opendir(destpath, prefix=None, suffix=None):

    for filename in dir(destpath, prefix=prefix, suffix=suffix):
        # read records from file
        yield open(destpath + "/" + filename, "rb")

