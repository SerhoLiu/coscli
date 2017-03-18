## coscli

coscli - 一个简单的腾讯云 COS 命令行工具, 当前仅支持 Python2

### 安装

使用 `pip`

    $ pip install coscli

升级

    $ pip install --upgrade coscli


### 使用

配置文件默认为 `/etc/coscli.cfg`, 可以使用环境变量 `COSCLI_CONFIG` 来更改位置,
或者直接使用 `--config` 参数进行配置

配置文件采用 INI 格式

    [cos]
    app_id=foo
    access_key_id=bar
    access_key_secret=foo
    region=bar

使用命令

    $ coscli --help


### 疑问

Github: https://github.com/SerhoLiu/coscli
