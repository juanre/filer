# filer --- Store and recover sets of files associated to elastic
#           tags.
#
# Version 0.2, August 2012
#
# Copyright (C) 2012 Juan Reyero (http://juanreyero.com).
#
# Licensed under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied.  See the License for the specific
# language governing permissions and limitations under the
# License.
#
"""
Store away and recover sets of files associated to elastic
tags.

Example:

>>> fl = Filer('store')
>>> fl.reset()
>>> fl.store_file_content('this content, company first',
...                       {'lang': 'es', 'company': 'first'},
...                       name='filer-first')
>>> fl.store_file_content('this content, company second',
...                       {'lang': 'es', 'company': 'second'},
...                       name='filer-second')
>>> fl.get_content_files({'lang': 'es'})[0]
'store/6b/3c0824d17fca756ebb6b2b0c07c158/content'
>>> fl.get_content_files({'lang': 'es'})[-1]
'store/7a/792da20efd5a2141eeed30affb01e6/content'
>>> fl.get_meta({'lang': 'es', 'company': 'first'})[0]['name']
'filer-first'
>>> fl.get_meta({'lang': 'es', 'company': 'first'})[-1]['name']
'filer-first'
>>> fl.get_meta({'lang': 'es'})[-1]['name']
'filer-first'
>>> fl.get_meta({'lang': 'es'})[-1]['tag']
{'lang': 'es', 'company': 'first'}
>>> fl.get({'lang': 'es', 'company': 'first'})[-1][1]['name']
'filer-first'
>>> fi = open(fl.get({'lang': 'es', 'company': 'first'})[-1][0])
>>> fi.read()
'this content, company first'
>>> fl.get_content({'lang': 'es', 'company': 'first'},
...                reader=lambda x, tag, name: open(x).read().upper())
['THIS CONTENT, COMPANY FIRST']
>>> ### The second time finds it cached
>>> fl.get_content({'lang': 'es', 'company': 'first'},
...                reader=lambda x, tag, name: open(x).read().upper())
['THIS CONTENT, COMPANY FIRST']
"""

import os, os.path
import hashlib
import time
try:
    import cPickle as pickle
except:
    import pickle

from elastag import ElasTag


def _shash(content):
    md5 = hashlib.md5()
    md5.update(content)
    return md5.hexdigest()


class LockError(Exception):
    pass


class Lock():
    def __init__(self, lockfile, max_wait=2):
        self.lockfile = lockfile
        self.max_wait = max_wait

    def __enter__(self):
        if os.path.exists(self.lockfile):
            i = 0
            while i < self.max_wait*10 and os.path.exists(self.lockfile):
                time.sleep(0.1)
                i += 1
        if os.path.exists(self.lockfile):
            raise LockError
        else:
            with open(self.lockfile, 'w') as l:
                l.write('1')

    def __exit__(self, type, value, traceback):
        if os.path.exists(self.lockfile):
            os.remove(self.lockfile)


class Filer(object):
    def __init__(self, store=os.path.join("/", "var", "filer")):
        self.set_store(store)

    def set_store(self, store):
        if not os.path.exists(store):
            os.makedirs(store)
        self.store = store
        self.tags_file = os.path.join(store, 'filer.pkl')
        self.__cache = {}

    def reset(self):
        import shutil
        shutil.rmtree(self.store)
        os.makedirs(self.store)
        self.__cache = {}

    def store_tag(self, tag, value):
        with Lock(os.path.join(self.store, 'lock')):
            if os.path.exists(self.tags_file):
                tags = pickle.load(open(self.tags_file, 'rb'))
            else:
                tags = ElasTag()
            tags.add(tag, value)
            pickle.dump(tags, open(self.tags_file, 'wb'))

    def unique_path(self, shash):
        path = os.path.join(self.store, shash[0:2], shash[2:])
        if not os.path.exists(path):
            os.makedirs(path)
        return path

    def content_file(self, shash):
        return os.path.join(self.unique_path(shash), 'content')

    def meta_file(self, shash):
        return os.path.join(self.unique_path(shash), 'meta.pkl')

    def store_file_content(self, content, tag, meta=None, name=None):
        """Stores the content of the file named name, associated to
        the dictionary tag.
        """
        shash = _shash(content)
        self.store_tag(tag, shash)
        with open(self.content_file(shash), 'wb') as f:
            f.write(content)
        if meta is None:
            meta = {}
        meta['name'] = name
        meta['tag'] = tag
        pickle.dump(meta, open(self.meta_file(shash), 'w'))

    def store_file(self, name, tag, meta=None):
        """Stores the content of the file named name, associated to
        the dictionary tag.
        """
        self.store_file_content(open(name).read(), tag, meta, name)

    def get_content_files(self, tag):
        """Returns a list of the files where the content corresponding
        to the dictionary tag has been stored.
        """
        tags = pickle.load(open(self.tags_file, 'rb'))
        return [self.content_file(shash) for shash in tags.bag(tag)]

    def get_meta(self, tag):
        """Returns a list of dictionaries with the metadata of all the
        files corresponding to the dictionary tag.  The metadata has
        two entries: name, the original file name, and tag, the
        original tag.
        """
        tags = pickle.load(open(self.tags_file, 'rb'))
        return [pickle.load(open(self.meta_file(shash), 'rb'))
                for shash in tags.bag(tag)]

    def get(self, tag):
        """Returns a list of tuples with the file where the content
        has been stored and the metadata.  The metadata is a
        dictionary with two fixed entries (name, the original file
        name, and tag, the original tag dictionary) plus any metadata
        that was assigned to the file when storing it.
        """
        tags = pickle.load(open(self.tags_file, 'rb'))
        return [(self.content_file(shash),
                 pickle.load(open(self.meta_file(shash), 'rb')))
                for shash in tags.bag(tag)]

    def file_content(self, fname, meta, reader=None):
        if fname in self.__cache and hook in self.__cache[fname]:
            return self.__cache[hook][fname]
        #content = open(fname, 'r').read()
        if reader is not None:
            content = reader(fname, **meta)
        else:
            content = open(fname).read()
        if reader not in self.__cache:
            self.__cache[reader] = {}
        self.__cache[reader][fname] = content
        return content

    def get_content(self, tag, reader=None):
        """Returns the content of the files corresponding to the
        dictionary tag.  If reader is not none it will be run on each
        file's name.
        """
        return [self.file_content(fname, meta, reader)
                for fname, meta in self.get(tag)]


def _test():
    import doctest
    doctest.testmod()

if __name__ == "__main__":
    _test()
