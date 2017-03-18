#!/usr/bin/env python
# -*- coding: utf-8 -*-

import click
import qcloud_cos as qcos
from ConfigParser import ConfigParser

from coscli import __version__
from coscli import command


class CliConfig(object):

    def __init__(self, config):
        cfg = ConfigParser()
        cfg.read(config)
        self.cos_config = {
            "appid": cfg.get("cos", "app_id"),
            "key": cfg.get("cos", "access_key_id"),
            "secret": cfg.get("cos", "access_key_secret"),
            "region": cfg.get("cos", "region")
        }

        self._check_cos_config()

    def _check_cos_config(self):
        try:
            int(self.cos_config["appid"])
        except (ValueError, KeyError):
            raise ValueError("app_id must int value")

        appid = int(self.cos_config["appid"])
        key = unicode(self.cos_config["key"])
        secret = unicode(self.cos_config["secret"])
        region = unicode(self.cos_config["region"])

        qcos.CosClient(appid, key, secret, region)


pass_config = click.make_pass_decorator(CliConfig)


@click.group()
@click.option("--config",
              type=click.Path(exists=True, readable=True),
              envvar="COSCLI_CONFIG",
              default="/etc/coscli.cfg",
              help="Changes the coscli config file.")
@click.version_option(__version__)
@click.pass_context
def cli(ctx, config):
    """
    Coscli is simple command line tool for qcloud cos
    """
    try:
        conf = CliConfig(config)
    except Exception as e:
        raise SystemExit("\ncos config error: %s" % e)

    ctx.obj = conf


@cli.command(name="ls")
@click.argument("uri", nargs=1)
@click.option("--human", "-h", is_flag=True, help="Enable human readable.")
@pass_config
def ls_command(config, uri, human):
    """
    List path file or directory
    """
    try:
        command.cos_ls(config.cos_config, uri, human)
    except Exception as e:
        raise SystemExit("\n%s" % e)


@cli.command(name="put")
@click.argument("src", nargs=-1)
@click.argument("uri", nargs=1)
@click.option("--force", "-f", is_flag=True, help="Enable overwrite exists.")
@click.option("--checksum", "-c", is_flag=True, help="Enable checksum check.")
@click.option("--p", default=1, help="Use parallel upload")
@pass_config
def put_command(config, src, uri, force, checksum, p):
    """
    Put local file or directory to COS
    """
    try:
        command.cos_put(config.cos_config, src, uri, force, checksum, p)
    except Exception as e:
        raise SystemExit("\n%s" % e)


@cli.command(name="get")
@click.argument("uri", nargs=1)
@click.argument("dst", nargs=1)
@click.option("--force", "-f", is_flag=True, help="Enable overwrite exists.")
@click.option("--skip", "-s", is_flag=True, help="Enable skip exists.")
@click.option("--checksum", "-c", is_flag=True, help="Enable checksum check.")
@click.option("--p", default=1, help="Use parallel download")
@pass_config
def get_command(config, uri, dst, force, skip, checksum, p):
    """
    Get COS file or directory to local
    """
    try:
        command.cos_get(config.cos_config, uri, dst, force, skip, checksum, p)
    except Exception as e:
        raise SystemExit("\n%s" % e)


@cli.command(name="del")
@click.argument("uri", nargs=1)
@click.option("--recursive", "-r", is_flag=True,
              help="Enable recursive delete.")
@click.option("--p", default=1, help="Use parallel delete")
@pass_config
def del_command(config, uri, recursive, p):
    """
    Delete COS file or directory
    """
    try:
        command.cos_del(config.cos_config, uri, recursive, p)
    except Exception as e:
        raise SystemExit("\n%s" % e)
