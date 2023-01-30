# -*- coding: UTF-8 -*-
import argparse
import configparser
import logging
import os
import re
import subprocess
import threading
import time
from logging import handlers

'''
1. 需要在consul中增加三个key
{mysql_hostname1:mysql_port}/weight:{权重}
{mysql_hostname2:mysql_port}/weight:{权重}
{mysql_hostname3:mysql_port}/weight:{权重}

2. 判断是否需要配置环境变量
执行命令 orchestrator-client -c topology -i {cluster-alias},如果报错，
则新建文件/etc/profile.d/orchestrator-client.sh
内容为
export ORCHESTRATOR_API="http://{host1_ip}:3000/api http:/{host2_ip}:3000/api http://{host3_ip}:3000/api"

3. 需要修改consul-template的模板
weight的值从consul里边获取
  server dqa001 omt-dqa001:61106 check inter 12000 rise 3 fall 3 weight {{key "omt-dqa001:61106/weight"}}
  server dqa002 omt-dqa002:61106 check inter 12000 rise 3 fall 3 weight {{key "omt-dqa002:61106/weight"}}
  server dqa003 omt-dqa003:61106 check inter 12000 rise 3 fall 3 weight {{key "omt-dqa003:61106/weight"}}
4. 主要功能在infoFromOrc里
如果节点不是主库且状态是'ro'，执行setSlaverRO设置为'ro'
如果节点是主库，consul里设置kv weight 为0
6. 如果出现a是b的主，b是c的主时，会出现双主的一个情况，如下，dqa002是dqa001的主，dqa003是dqa002的主
在这种情况下，就会出现consul里的即为主又为从的机器weight值一直变动，解决方法就是，遍历节点，只把输出为':0'的节点加入到self.cls_master
输出为其他的，不加入

(root@omt-dqm001 2021-12-27 18:54:13 /data/scripts/orchestrator)
# orchestrator-client -c which-master -i omt-dqa003:61106
:0
(root@omt-dqm001 2021-12-27 18:55:18 /data/scripts/orchestrator)
# orchestrator-client -c which-master -i omt-dqa001:61106
omt-dqa002:61106
(root@omt-dqm001 2021-12-27 18:55:27 /data/scripts/orchestrator)
# orchestrator-client -c which-master -i omt-dqa002:61106
omt-dqa003:61106

'''


class OrcHooks:
    def __init__(self, cfgfile):
        self.cls_tittle = ['ins_name', 'delay', 'ins_stat', 'mysql_version', 'mysql_mode', 'binlog_format', 'is',
                           'is_gtid']
        self.cls_ins = []
        self.ins_info = []
        self.cls_info = []
        self.cls_master: set = set()
        self.cls_name = []
        self.loglevels = {'DEBUG': logging.DEBUG, 'INFO': logging.INFO, 'ERROR': logging.ERROR, 'FATAL': logging.FATAL,
                          'CRITICAL': logging.CRITICAL, 'WARNING': logging.WARNING, 'WARN': logging.WARN,
                          'NOTSET': logging.NOTSET}

        cfgparser = configparser.ConfigParser()
        cfgparser.read(cfgfile, encoding='utf-8')
        slave_maxdelay = cfgparser.getint('orchestrator', 'slave_maxdelay')
        orchestrator_client = cfgparser.get('orchestrator', 'orchestrator_client')
        consul = cfgparser.get('consul', 'consul')
        consul_token_isset = cfgparser.getboolean('consul', 'consul_token_isset')
        consul_token = cfgparser.get('consul', 'consul_token')
        haproxy_cfg = cfgparser.get('haproxy', 'haproxy_cfg')

        loglevel = cfgparser.get('log', 'level')
        logfile_name = cfgparser.get('log', 'logfile_name')
        logfile_path = cfgparser.get('log', 'logfile_path')
        logfile_when = cfgparser.get('log', 'logfile_when')
        logfile_interval = cfgparser.getint('log', 'logfile_interval')
        logfile_maxcount = cfgparser.getint('log', 'logfile_maxcount')
        self.maxdelay = slave_maxdelay
        if consul_token_isset == True:
            self.consul_token = '--token {}'.format(consul_token)
        else:
            self.consul_token = ''

        if not os.path.exists(logfile_path):
            os.makedirs(logfile_path)
        self.logfile = logfile_path + '/' + logfile_name
        self.orchestrator_client = orchestrator_client
        self.consul = consul
        self.haproxy_cfg = haproxy_cfg

        # logger 写在init里
        self.fmt = logging.Formatter('%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
        self.logger = logging.getLogger(self.logfile)
        self.logger.setLevel(self.loglevels[loglevel])
        self.logger_handler = handlers.TimedRotatingFileHandler(filename=self.logfile, when=logfile_when,
                                                                backupCount=logfile_maxcount, encoding='utf-8',
                                                                interval=logfile_interval)
        # # filename：日志文件名的prefix；
        # # when：是一个字符串，用于描述滚动周期的基本单位，字符串的值及意义如下：
        # # “S”: Seconds
        # # “M”: Minutes
        # # “H”: Hours
        # # “D”: Days
        # # “W”: Week day (0=Monday)
        # # interval: 滚动周期，单位有when指定，比如：when='D',interval=1，表示每天产生一个日志文件；
        self.logger_handler.setFormatter(self.fmt)
        # # 如果对日志文件名没有特殊要求的话，可以不用设置suffix和extMatch，如果需要，一定要让它们匹配上。
        self.logger_handler.suffix = '%Y%m%d'
        self.logger_handler.extMatch = re.compile(r"^\d{8}$")
        self.logger.addHandler(self.logger_handler)

    def getClsName(self):
        # self.setLogger()
        self.cls_name.clear()
        self.cls_name = subprocess.getoutput(
            '{} -c clusters'.format(self.orchestrator_client)).split('\n')
        self.logger.info('The clusters\' names are {}'.format(
            self.cls_name.__str__()))

    def getServerStat(self):
        self.getClsName()
        self.cls_ins.clear()
        self.ins_info.clear()
        for clsname in self.cls_name:
            # # 设置主的weight为0
            server_status = subprocess.getoutput(
                '{} -c topology -i {}'.format(self.orchestrator_client, clsname))
            for line in server_status.split('\n'):
                line = line.strip().strip('+').strip('-').strip()
                # print(line) -- omt-dqa001:61106   [0s,ok,8.0.20-11,rw,ROW,>>,GTID]
                self.cls_ins.append(line.split(' ')[0])
                self.ins_info.append(line.split(
                    ' ')[-1].lstrip('[').rstrip(']').split(','))

    def getMasters(self):
        self.cls_master.clear()
        # print(self.cls_ins) -- ['omt-dqa001:61106', 'omt-dqa002:61106', 'omt-dqa003:61106']
        for instance in self.cls_ins:
            _master_host = subprocess.getoutput(
                '{} -c which-master -i {}'.format(self.orchestrator_client, instance))
            if _master_host == ':0':
                self.cls_master.add(instance)

        self.logger.info('Masters are {} !'.format(self.cls_master))

    def zipClsInfo(self):
        self.cls_info.clear()
        self.getServerStat()
        self.getMasters()
        vcls_ins = [[x] for x in self.cls_ins]
        for i in range(len(self.cls_ins)):
            vcls_insfo = vcls_ins[i] + self.ins_info[i]  # 两个列表做合并
            self.cls_info.append(dict(zip(self.cls_tittle, vcls_insfo)))

    def setMasterWeight(self, ins_name):
        master_weight = subprocess.getoutput(
            '{} kv get {} {}/weight'.format(self.consul, self.consul_token, ins_name))
        if int(master_weight) == 0:
            pass
        else:
            set_master_weight_stat = subprocess.getoutput(
                '{} kv put {} {}/weight 0'.format(self.consul, self.consul_token, ins_name))
            self.logger.warning(set_master_weight_stat)

    def printWeight(self):
        for ins_name in self.cls_ins:
            ins_weight = subprocess.getoutput(
                'consul kv get {} -recurse {}/weight '.format(self.consul_token, ins_name))
            print(ins_weight)

    def setSlaverWeight(self, ins_name):
        slaver_weight = subprocess.getoutput(
            '{} kv get {} {}/weight'.format(self.consul, self.consul_token, ins_name))
        if int(slaver_weight) == 10:
            pass
        else:
            self.logger.warning(
                'The instance {} is slaver,but the weight is not 10,begin set to 10'.format(ins_name))
            set_slaver_weight_stat = subprocess.getoutput(
                '{} kv put {} {}/weight 10'.format(self.consul, self.consul_token, ins_name))
            self.logger.warning(set_slaver_weight_stat)

    def infoFromOrc(self):
        self.zipClsInfo()
        # print(self.cls_info)
        try:
            for insinfo in self.cls_info:
                self.logger.info(insinfo)  # 记录节点状态信息
                delay_status: str = insinfo['delay']
                _is_readonly = insinfo['mysql_mode']
                _ins_name = insinfo['ins_name']

                # 判断是否是从库和状态是否是'rw'
                # 如果是主库则修改weight 值为0
                # 如果是从库则修改为'ro'
                if _ins_name not in self.cls_master and _is_readonly == 'rw':  # 如果是从库，并且为读写模式
                    self.logger.info('Begin to set slaver to read-only !')
                    # 修改为read-only模式
                    _salve_host = subprocess.getoutput(
                        '{} -c set-read-only -i {}'.format(self.orchestrator_client, _ins_name))
                    self.logger.warning(
                        'The {} has been set to read-only.'.format(_salve_host))

                # 如果是主,做判断，weight为0不用修改，如果不是0改为0
                elif _ins_name in self.cls_master:
                    # 设置主的weight为0
                    orchooks.setMasterWeight(ins_name=_ins_name)
                # 如果节点是从库，并且不是延迟从库。设置weight为10
                elif _ins_name not in self.cls_master and insinfo['ins_stat'] != 'lag':
                    orchooks.setSlaverWeight(ins_name=_ins_name)

                if delay_status.rstrip('s').isnumeric():  # 如果延迟是数字类型
                    self.ins_delay_time = int(delay_status.rstrip('s'))
                    if self.ins_delay_time > self.maxdelay:  # 如果延迟时间大于阈值
                        self.logger.critical(
                            '{} is High delay!'.format(_ins_name))
                        self.logger.warning(
                            'Begin to remove slave {}'.format(_ins_name))
                        self.monitorHaproxy(_ins_name, 'down')
                    elif insinfo['ins_stat'] == 'lag':  # 如果是延迟从库，一样配置为不可读。
                        self.monitorHaproxy(_ins_name, 'down')
                    else:
                        self.monitorHaproxy(_ins_name, 'up')

                else:  # 如果不是数字类型，可能是'unknown'等
                    self.logger.error('The instance {} is {} !'.format(
                        _ins_name, insinfo['ins_stat']))
                    self.monitorHaproxy(_ins_name, 'down')

        except Exception as err004:
            self.logger.critical('Error004:' + err004.__str__())
        finally:
            time.sleep(3)
            orchooks.orchookStart()

    def monitorHaproxy(self, delay_slaver_name, to_do):
        if to_do == 'down':
            high_weight_rownum = subprocess.getoutput(
                '/bin/grep --color=auto {} {}'.format(delay_slaver_name, self.haproxy_cfg)).count('weight 10')
            if high_weight_rownum != 0:
                # 不为0则表示需要修改consul里对应节点的权重
                # 获取更改是否成功，成功返回值为0，没成功则为其他，如果修改过一次之后，返回值可能为None
                return self.degradeSlave(delay_slaver_name)
            # else:  # 为0 时不需要重复修改，不写日志了，影响日志可读性
            #     self.logger.warning(
            #         'The server {} is already set weight lower!'.format(delay_slaver_name))
        else:
            low_weight_rownum = subprocess.getoutput(
                '/bin/grep --color=auto {} /etc/haproxy/haproxy.cfg'.format(delay_slaver_name)).count('weight 0')

            if delay_slaver_name not in self.cls_name and low_weight_rownum == 1:
                return self.upgradeSlave(delay_slaver_name)

    def degradeSlave(self, slaver_name):
        try:
            self.degrade_slave = subprocess.check_call(
                "{} kv put {} {}/weight 0".format(self.consul, self.consul_token, slaver_name), shell=True)
            if self.degrade_slave == 0:
                self.logger.warning(
                    'Modify consul kv {} succeed !'.format(slaver_name + '/weight'))
            else:
                self.logger.warning(
                    'The consul kv operation status : {:d} '.format(self.degrade_slave))
        except subprocess.CalledProcessError as err002:
            self.logger.error('Set consul key {} Failed,error code {} !'.format(
                slaver_name + '/weight', err002))

    def upgradeSlave(self, slaver_name):
        try:
            self.upgrade_slave = subprocess.check_call(
                "{} kv put {} {}/weight 10".format(self.consul, self.consul_token, slaver_name), shell=True)
            if self.degrade_slave == 0:
                self.logger.warning(
                    'Modify consul key {} succeed !'.format(slaver_name + '/weight'))
            else:
                self.logger.error(
                    'The consul kv operation status : {:d} '.format(self.degrade_slave))
        except subprocess.CalledProcessError as err005:
            self.logger.error(
                'Set haproxy.cfg Failed,error code %s !' % err005)

    def orchookStart(self):
        self.hooksthread = threading.Timer(2, orchooks.infoFromOrc)
        self.hooksthread.start()

    def setWeight(self):
        self.getServerStat()
        self.getMasters()
        for i in range(len(self.cls_ins)):
            ins_name = self.cls_ins[i]

            if ins_name in self.cls_master:
                print("{} is Master, set weight to 0".format(ins_name))
                subprocess.getoutput(
                    '{} kv put {} {}/weight 0'.format(self.consul, self.consul_token, ins_name))
                self.logger.warning('{} is Master, set weight to 0'.format(ins_name))

            else:
                if self.ins_info[i][1] == 'lag':
                    print('{} is Slaver, and the server is delay server,set weight to 0 !'.format(ins_name))
                    subprocess.getoutput(
                        '{} kv put {} {}/weight 0'.format(self.consul, self.consul_token, ins_name))
                    self.logger.warning(
                        '{} is Slaver, and the server is delay server,set weight to 0 !'.format(ins_name))
                else:
                    print('{} is Slaver, set weight to 10'.format(ins_name))
                    subprocess.getoutput(
                        '{} kv put {} {}/weight 10'.format(self.consul, self.consul_token, ins_name))
                    self.logger.warning('{} is Slaver, set weight to 10'.format(ins_name))


if __name__ == '__main__':
    _config = '''[orchestrator]
# mysql master and slave max delay time
slave_maxdelay = 20
# the orchestrator-client file path
orchestrator_client = /bin/orchestrator-client

[consul]
# the consul path
consul = /usr/local/bin/consul
consul_token_isset = true
consul_token = 6f4f17c4-1cad-4750-99fd-a053b4725953

[haproxy]
haproxy_cfg = /etc/haproxy/haproxy.cfg

[log]
# CRITICAL
# FATAL = CRITICAL
# ERROR
# WARNING
# WARN = WARNING
# INFO 
# DEBUG
# NOTSET
level = DEBUG
# logfile name
logfile_name = orc_hooks.log
# log file to store
logfile_path = /data/scripts/orchestrator
# logfile switch periodic
# when：是一个字符串，用于描述滚动周期的基本单位，字符串的值及意义如下：
# “S”: Seconds
# “M”: Minutes
# “H”: Hours
# “D”: Days
# “W”: Week day (0=Monday)
logfile_when = D
# interval: 滚动周期，单位有when指定，比如：when='D',interval=1，表示每天产生一个日志文件；
logfile_interval = 1
# file max count
logfile_maxcount = 20'''
    myargparse = argparse.ArgumentParser()
    myargparse.add_argument('--generate-config', '-g', help='generate the config file', action='store_true')
    myargparse.add_argument('--config', '-c', help='set the config file path', required=False,
                            default='orc_hooks.cfg.example')
    myargparse.add_argument('--set-consul-kv', help='set cluster info to consul(kv)', action='store_true',
                            default=False)
    myargparse.add_argument('--start', help='start orchooks', action='store_true', default=False)
    myargs = myargparse.parse_args()
    cfgfile: str = myargs.config
    if os.path.exists(cfgfile):
        pass
    else:
        print('The config file not exists!')
    orchooks = OrcHooks(cfgfile=cfgfile)
    if myargs.generate_config:
        with open(file=cfgfile, mode='w+', encoding='utf-8') as f:
            f.write(_config)

    if myargs.set_consul_kv:
        orchooks.setWeight()
    if myargs.start:
        try:
            orchooks.orchookStart()
        except Exception as err001:
            print(err001)
