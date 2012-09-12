#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Store away and recover sets of files associated to elastic
configurations.
"""
import os, os.path
import hashlib
import time
try:
    import cPickle as pickle
except:
    import pickle

from elasconf import ElasConf


def __shash(content):
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
            while i < self.max_wait*10 and os.path.exists(self.lockfile):
                time.sleep(0.1)
        if os.path.exists(self.lockfile):
            raise LockError
        else:
            with open(self.lockfile, 'w') as l:
                l.write('1')

    def __exit__(self, type, value, traceback):
        if os.path.exists(self.lockfile):
            os.remove(self.lockfile)


class Filer(object):
    def __init__(self, store=os.join("var", "filer")):
        if not os.path.exists(store):
            os.mkdirs(store)
        self.store = store
        self.tags_file = os.path.join(store, 'filer.pkl')

    def store_tag(self, tag, value):
        with Lock(os.join(self.store, 'lock')):
            if os.path.exists(self.tags_file):
                tags = pickle.load(self.tags_file)
            else:
                tags = ElasConf()
            tags.append(tag, value)
            pickle.dump(tags, open(self.tags_file, 'wb'))

    def unique_path(self, shash):
        path = os.path.join(self.store, shash[0:2], shash[2:])
        if not os.path.exists(path):
            os.mkdirs(path)
        return path

    def content_file(self, shash):
        return os.path.join(self.unique_path(shash), 'content')

    def meta_file(self, shash):
        return os.path.join(self.unique_path(shash), 'meta.pkl')

    def add_file_content(self, name, content, tag):
        shash = __shash(content)
        self.store_tag(tag, shash)
        with open(self.content_file(), 'wb') as f:
            f.write(content)
        pickle.dump({'name': name, 'tag': tag},
                    open(self.meta_file(), 'w'))

    def get_files(self, tag):
        tags = pickle.load(open(self.tags_file, 'rb'))
        return [self.content_file(shash) for shash in tags.all(tag)]
