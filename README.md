# orchestarator-hooks
A hook for orchestrator
# How to run
`python3 orc_consul_hook_with_logsuffix.py --help` to show help:
```
usage: orc_consul_hook_with_logsuffix.py [-h] [--generate-config]
                                         [--config CONFIG] [--set-consul-kv]
                                         [--start]

optional arguments:
  -h, --help            show this help message and exit
  --generate-config, -g
                        generate the config file
  --config CONFIG, -c CONFIG
                        set the config file path
  --set-consul-kv       set cluster info to consul(kv)
  --start               start orchooks
```
使用`python3 orc_consul_hook_with_logsuffix.py --set-consul-kv` 在consul中设置节点的权重.

使用`nohup python3 orc_consul_hook_with_logsuffix.py > /dev/null 2>&1 &`运行脚本，脚本会轮询检测节点的状态值，当发生主从切换的时候，会修改haproxy中节点的权重值，达到故障切换的作用.
