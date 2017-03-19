#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import time

from coscli.cos import COS
from coscli.utils import ThreadWorker
from coscli.utils import ensure_dir_exists, COSUri
from coscli.utils import output, format_size, sha1_checksum


class Uploader(object):

    def __init__(self, config, bucket, tasks, force, checksum):
        self.cos_config = config.cos_config
        self.dry_run = config.dry_run

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

        try:
            if self.dry_run:
                msg = "dry run"
            else:
                msg = self._do_upload(cos, task)
        except Exception as e:
            msg = str(e)

        output(sformat % (
            index, total,
            local_file, COSUri.compose_uri(self.bucket, cos_dest),
            msg
        ))

    def _do_upload(self, cos, task):
        local_file, cos_dest = task

        if not self.force:
            if cos.file_exists(self.bucket, cos_dest):
                return "error: dest exists"

        start = time.time()
        cos.upload(self.bucket, cos_dest, local_file)
        cost = time.time() - start

        local_size = os.path.getsize(local_file)
        cos_size, _, cos_sha1 = cos.stat_file(self.bucket, cos_dest)
        if local_size != cos_size:
            return "error: file size not match"

        if self.checksum:
            local_sha1 = sha1_checksum(local_file)
            if local_sha1 != cos_sha1:
                return "error: sha1 checksum not match"

        speed = local_size / cost
        value, coeff = format_size(speed, human_readable=True)
        msg = "%d bytes in %0.1f seconds, %0.2f%sB/s" % (
            local_size, cost, value, coeff
        )

        return msg


class Downloader(object):

    def __init__(self, config, bucket, tasks, force, skip, checksum):
        self.cos_config = config.cos_config
        self.dry_run = config.dry_run

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

        try:
            if self.dry_run:
                msg = "dry run"
            else:
                msg = self._do_download(cos, task)
        except Exception as e:
            msg = str(e)

        output(sformat % (
            index, total,
            COSUri.compose_uri(self.bucket, cos_path), local_file,
            msg
        ))

    def _do_download(self, cos, task):
        cos_path, local_file = task

        if os.path.exists(local_file):
            if self.skip:
                return "skip exists"
            if not self.force:
                return "error: local file exists"

        dirname = os.path.dirname(local_file)
        ensure_dir_exists(dirname)

        start = time.time()
        cos.download(self.bucket, cos_path, local_file)
        cost = time.time() - start

        local_size = os.path.getsize(local_file)
        cos_size, _, cos_sha1 = cos.stat_file(self.bucket, cos_path)
        if local_size != cos_size:
            return "error: file size not match"

        if self.checksum:
            local_sha1 = sha1_checksum(local_file)
            if local_sha1 != cos_sha1:
                return "error: sha1 checksum not match"

        speed = local_size / cost
        speed_fmt = format_size(speed, human_readable=True)
        msg = "%d bytes in %0.1f seconds, %0.2f%sB/s" % (
            local_size, cost, speed_fmt[0], speed_fmt[1]
        )

        return msg


class Deleter(object):

    def __init__(self, config, bucket, tasks):
        self.cos_config = config.cos_config
        self.dry_run = config.dry_run

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
        cos_path = task

        try:
            if self.dry_run:
                output("(%s/%s) deleted: %s (dry run)" % (
                    index, total,
                    COSUri.compose_uri(self.bucket, cos_path)
                ))
            else:
                cos.delete(self.bucket, cos_path)
                output("(%s/%s) deleted: %s" % (
                    index, total,
                    COSUri.compose_uri(self.bucket, cos_path)
                ))
        except Exception as e:
            output("(%s/%s) delete: %s (%s)" % (
                index, total,
                COSUri.compose_uri(self.bucket, cos_path),
                str(e)
            ))


class Mover(object):

    def __init__(self, config, bucket, tasks, force):
        self.cos_config = config.cos_config
        self.dry_run = config.dry_run

        self.bucket = bucket
        self.tasks = tasks
        self.force = force

    def simple_move(self):
        cos = COS(self.cos_config)

        total = len(self.tasks)
        for index, task in enumerate(self.tasks):
            self._move(total, index+1, cos, task)

    def parallel_move(self, count):

        def setup():
            return COS(self.cos_config)

        def work(ctx, job):
            cos = ctx
            _total, _index, _task = job
            self._move(_total, _index, cos, _task)

        worker = ThreadWorker(count, setup=setup, work=work)
        total = len(self.tasks)
        for index, task in enumerate(self.tasks):
            worker.add_job((total, index+1, task))

        worker.start()

    def _move(self, total, index, cos, task):
        sformat = "(%s/%s) mv: %s -> %s (%s)"
        cos_src, cos_dest = task

        try:
            if self.dry_run:
                msg = "dry run"
            else:
                msg = self._do_mv(cos, task)
        except Exception as e:
            msg = str(e)

        output(sformat % (
            index, total,
            COSUri.compose_uri(self.bucket, cos_src),
            COSUri.compose_uri(self.bucket, cos_dest),
            msg
        ))

    def _do_mv(self, cos, task):
        cos_src, cos_dest = task

        if not self.force:
            if cos.file_exists(self.bucket, cos_dest):
                return "error: dest exists"

        cos.move(self.bucket, cos_src, cos_dest)

        return "ok"
