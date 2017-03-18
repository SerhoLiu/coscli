#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import glob
import posixpath

from coscli.cos import COS
from coscli.tools import Uploader, Downloader, Deleter
from coscli.utils import COSUri, output
from coscli.utils import format_datetime, format_size, list_dir_files


def cos_ls(cos_config, uri, human):
    cos = COS(cos_config)
    cos_uri = COSUri(uri)

    if cos_uri.path.endswith("/"):
        if not cos.dir_exists(cos_uri.bucket, cos_uri.path):
            output("Dir '%s' not exists" % uri)
            return
        paths = list(cos.iter_path(cos_uri.bucket, cos_uri.path))
    else:
        if not cos.file_exists(cos_uri.bucket, cos_uri.path):
            output("File '%s' not exists" % uri)
            return

        paths = [cos_uri.path]

    # (mtime, size, name)
    records = []
    for path in paths:
        if path.endswith("/"):
            records.append((
                "1970-01-01 00:00:00",
                "DIR",
                COSUri.compose_uri(cos_uri.bucket, path)
            ))
        else:
            filesize, mtime, _ = cos.stat_file(cos_uri.bucket, path)
            size, coeff = format_size(filesize, human)
            records.append((
                format_datetime(mtime),
                "%s%s" % (size, coeff),
                COSUri.compose_uri(cos_uri.bucket, path)
            ))

    # dir first
    records.sort(key=lambda r: (r[0], r[2]))
    output("Found %s items" % len(records))
    for record in records:
        output("%19s %10s %s" % record)


def cos_put(cos_config, srcs, uri, force, checksum, p):
    cos = COS(cos_config)
    cos_uri = COSUri(uri)

    globs = []
    for src in srcs:
        globs.extend(glob.glob(src))

    total = 0
    local_files = []
    for path in globs:
        print path
        if os.path.isfile(path):
            files = [path]
            is_file = True
        elif os.path.isdir(path):
            is_file = False
            files = list(list_dir_files(path))
        else:
            continue

        if len(files) == 0:
            continue

        total += len(files)
        local_files.append((path, is_file, files))

    output("Found %s items to put" % total)

    if total == 0:
        return

    # put 到非目录, 需要先检查是否存在同名的目录
    if total == 1 and not cos_uri.path.endswith("/"):
        if cos.dir_exists(cos_uri.bucket, cos_uri.path):
            raise Exception("%s is dir, need end with '/'" % uri)

    # 多个文件只能 put 到目录下
    if total > 1 and not cos_uri.path.endswith("/"):
        raise Exception(
            "multiple files must put to dir, %s need endswith '/'" % uri
        )

    tasks = []
    for src, is_file, files in local_files:
        # 这里上传到 COS 的路径由以下方式决定
        # - src 是文件
        #   1. cos_uri.path 以 / 结束, 则 cos_uri.path/basename(src)
        #   2. 否则 cos_uri.path
        # - src 是目录
        #   1. src 以 / 结束, 则相当于将 src 内文件上传至 cos_uri.path
        #   2. 否则将 src 目录上传至 cos_uri.path
        for file_path in files:
            if is_file:
                if cos_uri.path.endswith("/"):
                    basename = os.path.basename(file_path)
                    dest = os.path.join(cos_uri.path, basename)
                else:
                    dest = cos_uri.path
            else:
                name = file_path[len(src):].lstrip(os.path.sep)
                if not src.endswith(os.path.sep):
                    dirname = os.path.basename(src)
                    name = os.path.join(dirname, name)
                dest = "/".join(
                    os.path.join(cos_uri.path, name).split(os.path.sep)
                )

            tasks.append((file_path, dest))

    uploader = Uploader(cos_config, cos_uri.bucket, tasks, force, checksum)
    if p > 1:
        uploader.parallel_upload(p)
    else:
        uploader.simple_upload()


def cos_get(cos_config, uri, dst, force, skip, checksum, p):
    cos = COS(cos_config)
    cos_uri = COSUri(uri)

    cos_files = []
    if cos.file_exists(cos_uri.bucket, cos_uri.path):
        is_file = True
        cos_files.append(cos_uri.path)
    elif cos.dir_exists(cos_uri.bucket, cos_uri.path):
        is_file = False
        for filepath in cos.walk_path(cos_uri.bucket, cos_uri.path):
            cos_files.append(filepath)
    else:
        output("Path '%s' not exists" % uri)
        return

    total = len(cos_files)
    output("Found %d items to download" % total)

    if total == 0:
        return

    tasks = []
    if not os.path.isdir(dst):
        if total > 1:
            raise Exception("dest must a dir when download multiple files.")
        tasks.append((cos_files[0], dst))
    elif os.path.isdir(dst):
        # 下载到本地的文件路径由以下方式决定
        # - cos path 是文件, 则 dst/basename(cos path)
        # - cos path 是文件夹, 则 dst/dir/filename

        prefix_len = len(posixpath.dirname(cos_uri.path.rstrip("/"))) + 1
        for cos_path in cos_files:
            if is_file:
                local_file = os.path.join(dst, posixpath.basename(cos_path))
            else:
                local_file = os.path.join(
                    dst, cos_path[prefix_len:].lstrip("/")
                )
                if os.path.sep != "/":
                    local_file = os.path.sep.join(local_file.split("/"))
            tasks.append((cos_path, local_file))
    else:
        raise Exception("WTF? Is it a dir or not? -- %s" % dst)

    downloader = Downloader(
        cos_config, cos_uri.bucket, tasks, force, skip, checksum
    )
    if p > 1:
        downloader.parallel_download(p)
    else:
        downloader.simple_download()


def cos_del(cos_config, uri, recursive, p):
    cos = COS(cos_config)
    cos_uri = COSUri(uri)

    cos_files = []
    if cos.file_exists(cos_uri.bucket, cos_uri.path):
        cos_files.append(cos_uri.path)
    elif cos.dir_exists(cos_uri.bucket, cos_uri.path):
        if not recursive:
            output("Path '%s' is dir, use --recursive/-r" % uri)
            return
        for filepath in cos.walk_path(cos_uri.bucket, cos_uri.path):
            cos_files.append(filepath)
    else:
        output("Path '%s' not exists" % uri)
        return

    total = len(cos_files)
    output("Found %d items to delete" % total)

    if total == 0:
        return

    deleter = Deleter(cos_config, cos_uri.bucket, cos_files)
    if p > 1:
        deleter.parallel_delete(p)
    else:
        deleter.simple_delete()
