#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
import click
import errno
import Queue
import hashlib
import os.path
import datetime
import threading


def output(info):
    click.echo(info)


class COSUri(object):

    _re = re.compile("^cosn:///*([^/]*)/?(.*)", re.IGNORECASE | re.UNICODE)

    def __init__(self, uri):
        match = self._re.match(uri)
        if not match:
            raise ValueError("%s: not a COS URI" % uri)

        groups = match.groups()

        self.bucket = groups[0]
        if not self.bucket:
            raise ValueError("%s: no bucket" % uri)

        path = groups[1]
        self.path = "/" + path.lstrip("/")

    def uri(self):
        return "cosn://%s%s" % (self.bucket, self.path)

    @staticmethod
    def compose_uri(bucket, path="/"):
        if path.startswith("/"):
            return "cosn://%s%s" % (bucket, path)
        else:
            return "cosn://%s/%s" % (bucket, path)


def format_datetime(timestamp):
    dt = datetime.datetime.fromtimestamp(timestamp)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def format_size(size, human_readable=False):
    if human_readable:
        size = float(size)
        coeffs = ["k", "M", "G", "T"]
        coeff = ""
        while size > 2048:
            size /= 1024
            coeff = coeffs.pop(0)
        return round(size, 1), coeff
    else:
        return size, ""


def ensure_dir_exists(dirname):
    try:
        os.makedirs(dirname)
    except OSError as err:
        if err.errno == errno.EEXIST and os.path.isdir(dirname):
            pass
        else:
            raise


def list_dir_files(path):
    """
    List all files in give directory
    :param path: directory
    """
    listdir_names = os.listdir(path)
    names = []
    for name in listdir_names:
        file_path = os.path.join(path, name)
        if os.path.isdir(file_path):
            name = name + os.path.sep
        names.append(name)

    names.sort(key=lambda item: item.replace(os.sep, "/"))
    for name in names:
        file_path = os.path.join(path, name)
        if os.path.isdir(file_path):
            for x in list_dir_files(file_path):
                yield x
        elif os.path.isfile(file_path):
            yield file_path


def sha1_checksum(filepath):
    """
    Calc sha1 checksum
    """
    sha1 = hashlib.sha1()
    with open(filepath, "rb") as f:
        while True:
            data = f.read(64 * 1024)
            if not data:
                break
            sha1.update(data)

    return sha1.hexdigest()


class ThreadWorker(object):

    def __init__(self, nworker, setup=None, work=None):
        self._nworker = nworker
        self._setup = setup
        self._work = work
        self._queue = Queue.Queue()

    def add_job(self, job):
        self._queue.put(job)

    def start(self):
        threads = []
        for i in range(self._nworker):
            thread = threading.Thread(
                target=self._do_work,
            )
            threads.append(thread)
            thread.start()

        for thread in reversed(threads):
            thread.join()

    def _do_work(self):
        if self._setup:
            ctx = self._setup()
        else:
            ctx = None

        while True:
            try:
                job = self._queue.get(timeout=1)
            except Queue.Empty:
                break

            self._work(ctx, job)
