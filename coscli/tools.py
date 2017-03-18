#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import time
from coscli.cos import COS

from coscli.utils import ThreadWorker
from coscli.utils import ensure_dir_exists, COSUri
from coscli.utils import output, format_size, sha1_checksum


class Uploader(object):

    def __init__(self, cos_config, bucket, tasks, force, checksum):
        self.cos_config = cos_config
        self.bucket = bucket
        self.tasks = tasks
        self.force = force
        self.checksum = checksum

    def simple_upload(self):
        cos = COS(self.cos_config)

        total = len(self.tasks)
        for index, task in enumerate(self.tasks):
            self._upload(total, index+1, cos, task)

    def parallel_upload(self, count):

        def setup():
            return COS(self.cos_config)

        def work(ctx, job):
            cos = ctx
            _total, _index, _task = job
            self._upload(_total, _index, cos, _task)

        worker = ThreadWorker(count, setup=setup, work=work)
        total = len(self.tasks)
        for index, task in enumerate(self.tasks):
            worker.add_job((total, index+1, task))

        worker.start()

    def _upload(self, total, index, cos, task):
        sformat = "(%s/%s) upload: %s -> %s (%s)"
        local_file, cos_dest = task

        if not self.force:
            if cos.file_exists(self.bucket, cos_dest):
                output(sformat % (
                    index, total,
                    local_file, COSUri.compose_uri(self.bucket, cos_dest),
                    "error: dest exists"
                ))
                return

        start = time.time()
        cos.upload(self.bucket, cos_dest, local_file)
        cost = time.time() - start

        local_size = os.path.getsize(local_file)
        cos_size, _, cos_sha1 = cos.stat_file(self.bucket, cos_dest)
        if local_size != cos_size:
            output(sformat % (
                index, total,
                local_file, COSUri.compose_uri(self.bucket, cos_dest),
                "error: file size not match"
            ))
            return

        if self.checksum:
            local_sha1 = sha1_checksum(local_file)
            if local_sha1 != cos_sha1:
                output(sformat % (
                    index, total,
                    local_file, COSUri.compose_uri(self.bucket, cos_dest),
                    "error: sha1 checksum not match"
                ))
                return

        speed = local_size / cost
        speed_fmt = format_size(speed, human_readable=True)
        msg = "%d bytes in %0.1f seconds, %0.2f%sB/s" % (
            local_size, cost, speed_fmt[0], speed_fmt[1]
        )
        output(sformat % (
            index, total,
            local_file, COSUri.compose_uri(self.bucket, cos_dest),
            msg
        ))


class Downloader(object):

    def __init__(self, cos_config, bucket, tasks, force, skip, checksum):
        self.cos_config = cos_config
        self.bucket = bucket
        self.tasks = tasks
        self.force = force
        self.skip = skip
        self.checksum = checksum

    def simple_download(self):
        cos = COS(self.cos_config)

        total = len(self.tasks)
        for index, task in enumerate(self.tasks):
            self._download(total, index+1, cos, task)

    def parallel_download(self, count):

        def setup():
            return COS(self.cos_config)

        def work(ctx, job):
            cos = ctx
            _total, _index, _task = job
            self._download(_total, _index, cos, _task)

        worker = ThreadWorker(count, setup=setup, work=work)
        total = len(self.tasks)
        for index, task in enumerate(self.tasks):
            worker.add_job((total, index+1, task))

        worker.start()

    def _download(self, total, index, cos, task):
        sformat = "(%s/%s) download: %s -> %s (%s)"
        cos_path, local_file = task

        if os.path.exists(local_file):
            if self.skip:
                output(sformat % (
                    index, total,
                    COSUri.compose_uri(self.bucket, cos_path), local_file,
                    "skip exists"
                ))
                return
            if not self.force:
                output(sformat % (
                    index, total,
                    COSUri.compose_uri(self.bucket, cos_path), local_file,
                    "error: local file exists"
                ))
                return

        dirname = os.path.dirname(local_file)
        ensure_dir_exists(dirname)

        start = time.time()
        cos.download(self.bucket, cos_path, local_file)
        cost = time.time() - start

        local_size = os.path.getsize(local_file)
        cos_size, _, cos_sha1 = cos.stat_file(self.bucket, cos_path)
        if local_size != cos_size:
            output(sformat % (
                index, total,
                COSUri.compose_uri(self.bucket, cos_path), local_file,
                "error: file size not match"
            ))
            return

        if self.checksum:
            local_sha1 = sha1_checksum(local_file)
            if local_sha1 != cos_sha1:
                output(sformat % (
                    index, total,
                    COSUri.compose_uri(self.bucket, cos_path), local_file,
                    "error: sha1 checksum not match"
                ))
                return

        speed = local_size / cost
        speed_fmt = format_size(speed, human_readable=True)
        msg = "%d bytes in %0.1f seconds, %0.2f%sB/s" % (
            local_size, cost, speed_fmt[0], speed_fmt[1]
        )
        output(sformat % (
            index, total,
            COSUri.compose_uri(self.bucket, cos_path), local_file,
            msg
        ))


class Deleter(object):

    def __init__(self, cos_config, bucket, tasks):
        self.cos_config = cos_config
        self.bucket = bucket
        self.tasks = tasks

    def simple_delete(self):
        cos = COS(self.cos_config)

        total = len(self.tasks)
        for index, task in enumerate(self.tasks):
            self._delete(total, index+1, cos, task)

    def parallel_delete(self, count):

        def setup():
            return COS(self.cos_config)

        def work(ctx, job):
            cos = ctx
            _total, _index, _task = job
            self._delete(_total, _index, cos, _task)

        worker = ThreadWorker(count, setup=setup, work=work)
        total = len(self.tasks)
        for index, task in enumerate(self.tasks):
            worker.add_job((total, index+1, task))

        worker.start()

    def _delete(self, total, index, cos, task):
        sformat = "(%s/%s) deleted: %s"
        cos_path = task

        cos.delete(self.bucket, cos_path)
        output(sformat % (
            index, total,
            COSUri.compose_uri(self.bucket, cos_path)
        ))
