#!/usr/bin/env python
# -*- coding: utf-8 -*-

import posixpath
import qcloud_cos as qcos


class COS(object):

    def __init__(self, config):
        appid = int(config["appid"])
        key = unicode(config["key"])
        secret = unicode(config["secret"])
        region = unicode(config["region"])

        self.client = qcos.CosClient(appid, key, secret, region)

    def file_exists(self, bucket, path):
        """
        文件是否存在 COS 上

        :param bucket: bucket name
        :param path: file path
        """
        req = qcos.StatFileRequest(unicode(bucket), unicode(path))
        resp = self.client.stat_file(req)

        return resp["code"] == 0

    def dir_exists(self, bucket, path):
        """
        目录是否存在 COS 上

        :param bucket: bucket name
        :param path: dir path
        """
        # COS 没办法存储一个空目录, 判断一个目录是否存在需要检查下面是否有文件
        dir_path = path.rstrip("/") + "/"
        req = qcos.ListFolderRequest(
            unicode(bucket), unicode(dir_path), num=1
        )
        resp = self.client.list_folder(req)
        if resp["code"] != 0:
            raise Exception(resp["message"])

        if len(resp["data"]["infos"]) != 0:
            return True

        return not resp["data"]["listover"]

    def iter_path(self, bucket, path):
        """
        列出目录下所有文件和目录

        :param bucket: bucket name
        :param path: dir path
        """
        data = {"listover": False, "context": u""}
        while not data["listover"]:
            req = qcos.ListFolderRequest(
                unicode(bucket),
                unicode(path), context=data["context"]
            )
            resp = self.client.list_folder(req)
            if resp["code"] != 0:
                raise Exception(resp["message"])
            data = resp["data"]
            for info in data["infos"]:
                yield posixpath.join(path, info["name"])

    def walk_path(self, bucket, path):
        """
        递归的列出目录下所有文件

        :param bucket: bucket name
        :param path: dir path
        """
        for subpath in self.iter_path(bucket, path):
            if subpath.endswith("/"):
                for filepath in self.walk_path(bucket, subpath):
                    yield filepath
            else:
                yield subpath

    def upload(self, bucket, path, local_file):
        """
        上传本地文件到 COS, 将覆盖已经存在的文件

        :param bucket: bucket name
        :param path: dest cos path
        :param local_file: local file
        """
        req = qcos.UploadFileRequest(
            unicode(bucket),
            unicode(path),
            unicode(local_file),
            insert_only=0
        )
        resp = self.client.upload_file(req)
        if resp["code"] != 0:
            # COS 上传失败的文件会保留, 下次上传时无法覆盖, 这里先删除
            if resp["message"].find("status_code:403") != -1:
                req = qcos.DelFileRequest(unicode(bucket), unicode(path))
                self.client.del_file(req)

            raise Exception(resp["message"])

    def download(self, bucket, path, local_file):
        """
        下载 COS 文件到本地

        :param bucket: bucket name
        :param path: cos path
        :param local_file: dest local file
        """
        req = qcos.DownloadFileRequest(
            unicode(bucket), unicode(path), unicode(local_file)
        )
        resp = self.client.download_file(req)
        if resp["code"] != 0:
            raise Exception(resp["message"])

    def delete(self, bucket, path):
        """
        删除 COS 文件

        :param bucket: bucket name
        :param path: cos path
        """
        req = qcos.DelFileRequest(unicode(bucket), unicode(path))
        resp = self.client.del_file(req)
        if resp["code"] != 0:
            raise Exception(resp["message"])

    def move(self, bucket, src_path, dest_path):
        """
        移动 COS 文件, 将覆盖已存在的文件

        :param bucket: bucket name
        :param src_path: src cos path
        :param dest_path: dest cos path
        """
        req = qcos.MoveFileRequest(
            unicode(bucket),
            unicode(src_path),
            unicode(dest_path),
            overwrite=True
        )
        resp = self.client.move_file(req)
        if resp["code"] != 0:
            raise Exception(resp["message"])

    def stat_file(self, bucket, path):
        """
        获取 COS 文件属性

        :param bucket: bucket name
        :param path: cos path
        """
        req = qcos.StatFileRequest(unicode(bucket), unicode(path))
        resp = self.client.stat_file(req)
        if resp["code"] != 0:
            raise Exception(resp["message"])

        info = resp["data"]

        return info["filesize"], info["mtime"], info["sha"]
