#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import glob
import posixpath

from coscli.cos import COS, COSObject
from coscli.utils import COSUri, output
from coscli.utils import format_datetime, format_size, list_dir_files
from coscli.tools import Uploader, Downloader, Deleter, MoveCopyer


def _cos_obj_output(obj, bucket, human):
    """
    :type obj: COSObject
    """
    if obj.is_dir:
        output("%19s %10s  %s" % (
            "1970-01-01 00:00:00",
            "DIR",
            COSUri.compose_uri(bucket, obj.path)
        ))
    else:
        size, coeff = format_size(obj.filesize, human)
        output("%19s %10s  %s" % (
            format_datetime(obj.mtime),
            "%s%s" % (size, coeff),
            COSUri.compose_uri(bucket, obj.path)
        ))


def cos_ls(config, uri, recursive, human):
    cos = COS(config.cos_config)
    cos_uri = COSUri(uri)

    if cos.file_exists(cos_uri.bucket, cos_uri.path):
        cos_objs = [cos.stat_file(cos_uri.bucket, cos_uri.path)]
    elif cos.dir_exists(cos_uri.bucket, cos_uri.path):
        if not cos_uri.path.endswith("/"):
            cos_uri.path += "/"

        if recursive:
            total = 0
            for obj in cos.walk_path(cos_uri.bucket, cos_uri.path):
                total += 1
                _cos_obj_output(obj, cos_uri.bucket, human)
            output("Found %s items" % total)
            return

        cos_objs = list(cos.iter_path(cos_uri.bucket, cos_uri.path))
    else:
        output("Path '%s' not exists" % uri)
        return

    output("Found %s items" % len(cos_objs))

    cos_objs.sort(key=lambda x: x.ls_cmp_key())
    for obj in cos_objs:
        _cos_obj_output(obj, cos_uri.bucket, human)


def cos_put(config, srcs, uri, force, checksum, p):
    cos_uri = COSUri(uri)

    globs = []
    for src in srcs:
        globs.extend(glob.glob(src))

    total = 0
    local_files = []
    for path in globs:
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

    # 多个文件只能 put 到目录下
    if total > 1 and not cos_uri.path.endswith("/"):
        output(
            "multiple files must put to dir, %s need endswith '/'" % uri
        )
        return

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
                    dest = posixpath.join(cos_uri.path, basename)
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

    uploader = Uploader(config, cos_uri.bucket, tasks, force, checksum)
    if p > 1:
        uploader.parallel_upload(p)
    else:
        uploader.simple_upload()


def cos_get(config, uri, dst, force, skip, checksum, p):
    cos = COS(config.cos_config)
    cos_uri = COSUri(uri)

    cos_objs = []
    if cos.file_exists(cos_uri.bucket, cos_uri.path):
        is_file = True
        cos_objs.append(cos.stat_file(cos_uri.bucket, cos_uri.path))
    elif cos.dir_exists(cos_uri.bucket, cos_uri.path):
        is_file = False
        if not cos_uri.path.endswith("/"):
            cos_uri.path += "/"
        for obj in cos.walk_path(cos_uri.bucket, cos_uri.path):
            cos_objs.append(obj)
    else:
        output("Path '%s' not exists" % uri)
        return

    total = len(cos_objs)
    output("Found %d items to download" % total)

    if total == 0:
        return

    tasks = []
    if not os.path.isdir(dst):
        if total > 1:
            raise Exception("dest must a dir when download multiple files.")
        tasks.append((cos_objs[0], dst))
    elif os.path.isdir(dst):
        # 下载到本地的文件路径由以下方式决定
        # - cos path 是文件, 则 dst/basename(cos path)
        # - cos path 是文件夹, 则 dst/dir/filename

        prefix_len = len(posixpath.dirname(cos_uri.path.rstrip("/")))
        for obj in cos_objs:
            if is_file:
                local_file = os.path.join(dst, posixpath.basename(obj.path))
            else:
                local_file = os.path.join(
                    dst, obj.path[prefix_len:].lstrip("/")
                )
                if os.path.sep != "/":
                    local_file = os.path.sep.join(local_file.split("/"))
            tasks.append((obj, local_file))
    else:
        raise Exception("WTF? Is it a dir or not? -- %s" % dst)

    downloader = Downloader(
        config, cos_uri.bucket, tasks, force, skip, checksum
    )
    if p > 1:
        downloader.parallel_download(p)
    else:
        downloader.simple_download()


def cos_del(config, uri, recursive, p):
    cos = COS(config.cos_config)
    cos_uri = COSUri(uri)

    cos_files = []
    if cos.file_exists(cos_uri.bucket, cos_uri.path):
        cos_files.append(cos_uri.path)
    elif cos.dir_exists(cos_uri.bucket, cos_uri.path):
        if not recursive:
            output("Path '%s' is dir, use --recursive/-r" % uri)
            return

        if not cos_uri.path.endswith("/"):
            cos_uri.path += "/"
        for obj in cos.walk_path(cos_uri.bucket, cos_uri.path):
            cos_files.append(obj.path)
    else:
        output("Path '%s' not exists" % uri)
        return

    total = len(cos_files)
    output("Found %d items to delete" % total)

    if total == 0:
        return

    deleter = Deleter(config, cos_uri.bucket, cos_files)
    if p > 1:
        deleter.parallel_delete(p)
    else:
        deleter.simple_delete()


def cos_mv_copy(action, config, usrc, udst, force, recursive, p):
    if action not in ("mv", "copy"):
        raise Exception("not support '%s' action" % action)

    cos = COS(config.cos_config)
    src_uri = COSUri(usrc)
    dst_uri = COSUri(udst)

    if src_uri.bucket != dst_uri.bucket:
        output("Cos %s should in same bucket" % action)
        return

    cos_files = []
    if cos.file_exists(src_uri.bucket, src_uri.path):
        is_file = True
        cos_files.append(src_uri.path)
    elif cos.dir_exists(src_uri.bucket, src_uri.path):
        if not recursive:
            output("Path '%s' is dir, use --recursive/-r" % usrc)
            return

        if not dst_uri.path.endswith("/"):
            output("Dest '%s' must dir, need endswith '/'" % udst)
            return

        is_file = False
        if not src_uri.path.endswith("/"):
            src_uri.path += "/"
        for obj in cos.walk_path(src_uri.bucket, src_uri.path):
            cos_files.append(obj.path)
    else:
        output("Path '%s' not exists" % usrc)
        return

    total = len(cos_files)
    output("Found %d items to %s" % (total, action))

    if total == 0:
        return

    tasks = []
    prefix_len = len(posixpath.dirname(src_uri.path.rstrip("/")))
    for cos_file in cos_files:
        if is_file:
            if dst_uri.path.endswith("/"):
                basename = posixpath.basename(cos_file)
                dest = posixpath.join(dst_uri.path, basename)
            else:
                dest = dst_uri.path
        else:
            name = cos_file[prefix_len:].lstrip(os.path.sep)
            dest = posixpath.join(dst_uri.path, name)

        tasks.append((cos_file, dest))

    mover = MoveCopyer(action, config, src_uri.bucket, tasks, force)
    if p > 1:
        mover.parallel_move_copy(p)
    else:
        mover.simple_move_copy()


def cos_du(config, uri, s, human):
    cos = COS(config.cos_config)
    cos_uri = COSUri(uri)

    if cos.file_exists(cos_uri.bucket, cos_uri.path):
        cos_objs = [cos.stat_file(cos_uri.bucket, cos_uri.path)]
    elif cos.dir_exists(cos_uri.bucket, cos_uri.path):
        if not cos_uri.path.endswith("/"):
            cos_uri.path += "/"
        if s:
            cos_objs = cos.iter_path(cos_uri.bucket, cos_uri.path)
        else:
            cos_objs = [COSObject(cos_uri.path)]
    else:
        output("Path '%s' not exists" % uri)
        return

    records = []
    for cos_obj in cos_objs:
        size = 0
        if cos_obj.is_dir:
            for obj in cos.walk_path(cos_uri.bucket, cos_obj.path):
                size += obj.filesize
        else:
            size = cos_obj.filesize

        records.append((cos_obj, size))

    records.sort(key=lambda x: x[0].ls_cmp_key())
    for obj, size in records:
        size, coeff = format_size(size, human)
        output("%10s  %s" % (
            "%s%s" % (size, coeff),
            COSUri.compose_uri(cos_uri.bucket, obj.path)
        ))


def cos_test(config, uri, d, e, f):
    cos = COS(config.cos_config)
    cos_uri = COSUri(uri)

    file_exists = cos.file_exists(cos_uri.bucket, cos_uri.path)
    if f:
        return file_exists

    dir_exists = cos.dir_exists(cos_uri.bucket, cos_uri.path)
    if d:
        return dir_exists

    if e:
        return file_exists or dir_exists
