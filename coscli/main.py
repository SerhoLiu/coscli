#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import click
import os.path
import qcloud_cos as qcos
from ConfigParser import ConfigParser

from coscli import __version__
from coscli import command


SYSTEM_LEVEL_CONFIG = "/etc/coscli.cfg"
USER_LEVEL_CONFIG = "~/.coscli.cfg"


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

        self.dry_run = False
        self.debug = False

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


def handle_exception(e, debug):
    if debug:
        exc_type, exc_value, tb = sys.exc_info()
        assert exc_value is e
        raise exc_type, exc_value, tb
    else:
        raise SystemExit("\n%s" % e)


pass_config = click.make_pass_decorator(CliConfig)


@click.group()
@click.option("--config", type=click.Path(), envvar="COSCLI_CONFIG",
              help="Changes the coscli config file.")
@click.option("--dryrun", "-n", is_flag=True,
              help="Only show what should be do.")
@click.option("--debug", "-d", is_flag=True, help="Enable debug output.")
@click.version_option(__version__)
@click.pass_context
def cli(ctx, config, dryrun, debug):
    """
    Coscli is simple command line tool for qcloud cos
    """
    try:
        if config is None:
            config = os.path.expanduser(USER_LEVEL_CONFIG)
            if not os.path.exists(config):
                config = SYSTEM_LEVEL_CONFIG

        # check is readable
        with open(config) as f:
            f.read()

        conf = CliConfig(config)

        conf.dry_run = dryrun
        conf.debug = debug
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
        command.cos_ls(config, uri, human)
    except Exception as e:
        handle_exception(e, config.debug)


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
        command.cos_put(config, src, uri, force, checksum, p)
    except Exception as e:
        handle_exception(e, config.debug)


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
        command.cos_get(config, uri, dst, force, skip, checksum, p)
    except Exception as e:
        handle_exception(e, config.debug)


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
        command.cos_del(config, uri, recursive, p)
    except Exception as e:
        handle_exception(e, config.debug)


@cli.command(name="mv")
@click.argument("usrc", nargs=1)
@click.argument("udst", nargs=1)
@click.option("--force", "-f", is_flag=True, help="Enable overwrite exists.")
@click.option("--recursive", "-r", is_flag=True, help="Enable recursive mv.")
@click.option("--p", default=1, help="Use parallel download")
@pass_config
def mv_command(config, usrc, udst, force, recursive, p):
    """
    Mv COS file or directory to other COS local
    """
    try:
        command.cos_mv(config, usrc, udst, force, recursive, p)
    except Exception as e:
        handle_exception(e, config.debug)
