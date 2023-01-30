import logging
import os
import subprocess
import threading
import time


class OrcHooks:
    def __init__(self):
        self.cls_tittle = ['ins_name', 'delay', 'is_ok', 'mysql_version', 'mysql_mode', 'binlog_format', 'is',
                           'is_gtid']
        self.cls_ins = []
        self.ins_info = []
        self.cls_info = []
        self.cls_name = []
        self.maxdelay = 20
        self.cdate = time.strftime('%Y%m%d', time.localtime())
        if not os.path.exists('/data/scripts/orchestrator'):
            os.makedirs('/data/scripts/orchestrator')
        self.logfile = '/data/scripts/orchestrator/lcxorchooks.log.' + self.cdate

    def setLogger(self):
        logging.basicConfig(level=logging.DEBUG, filename=self.logfile, filemode='a+',
                            format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
        self.logger = logging.getLogger()

    def getClsName(self):
        self.setLogger()
        self.cls_name.clear()
        self.cls_name = subprocess.getoutput('orchestrator-client -c clusters').split('\n')
        self.logger.info('The clusters\' names are {}'.format(self.cls_name.__str__()))

    def getServerStat(self):
        self.getClsName()
        self.cls_ins.clear()
        self.ins_info.clear()
        for cls in self.cls_name:
            server_status = subprocess.getoutput('orchestrator-client -c topology -i {}'.format(cls))
            for line in server_status.split('\n'):
                line = line.strip('+').strip('-').strip()
                self.cls_ins.append(line.split(' ')[0])
                self.ins_info.append(line.split(' ')[-1].lstrip('[').rstrip(']').split(','))

    def zipClsInfo(self):
        self.cls_info.clear()
        self.getServerStat()
        vcls_ins = [[x] for x in self.cls_ins]
        for i in range(len(self.cls_ins)):
            vcls_insfo = vcls_ins[i] + self.ins_info[i]     # 两个列表做合并
            self.cls_info.append(dict(zip(self.cls_tittle, vcls_insfo)))

    def infoFromOrc(self):
        self.zipClsInfo()
        # print(self.cls_info)
        try:
            for insinfo in self.cls_info:
                self.logger.info(insinfo)
                delay_status: str = insinfo['delay']
                self.delay_slaver_name = insinfo['ins_name']
                if delay_status.rstrip('s').isnumeric():  # 如果延迟是数字类型
                    self.ins_delay_time = int(delay_status.rstrip('s'))
                    if self.ins_delay_time > self.maxdelay:  # 如果延迟时间大于阈值

                        self.logger.critical('{} is High delay!'.format(self.delay_slaver_name))
                        self.logger.warning('Begin to remove slave {}'.format(self.delay_slaver_name))
                        self.testHaproxy(self.delay_slaver_name, 'down')
                    else:
                        self.testHaproxy(self.delay_slaver_name, 'up')

                else:  # 如果不是数字类型，可能是'unknown'等
                    self.logger.error('The instance {} is {} !'.format(self.delay_slaver_name, insinfo['is_ok']))
                    self.testHaproxy(self.delay_slaver_name, 'down')

        except Exception as err004:
            self.logger.critical('Error004:' + err004.__str__())
        finally:
            time.sleep(3)
            orchooks.orchookStart()

    def testHaproxy(self, delay_slaver_name, to_do):
        if to_do == 'down':
            high_weight_rownum = subprocess.getoutput(
                'grep {} /etc/haproxy/haproxy.cfg'.format(delay_slaver_name)).count('weight 10')
            if high_weight_rownum != 0:  # 不为0则表示需要修改haproxy.cfg对应节点的权重
                # 获取更改是否成功，成功返回值为0，没成功则为其他，如果修改过一次之后，返回值可能为None
                return self.degradeSlave(delay_slaver_name)
            else:  # 为0 时不需要重复修改
                self.logger.warning('The server {} is already set weight lower!'.format(delay_slaver_name))
        else:
            low_weight_rownum = subprocess.getoutput(
                'grep {} /etc/haproxy/haproxy.cfg'.format(delay_slaver_name)).count('weight 0')
            # print('low_weight_rownum', low_weight_rownum)
            if delay_slaver_name not in self.cls_name and low_weight_rownum == 1:
                return self.upgradeSlave(delay_slaver_name)

    def degradeSlave(self, slaver_name):
        try:
            self.degrade_slave = subprocess.check_call(
                "sed -i '/{}/s/weight 10/weight 0/g' /etc/haproxy/haproxy.cfg".format(slaver_name), shell=True)
            if self.degrade_slave == 0:
                self.logger.warning('modify haproxy.cfg succeed, begin to reload haproxy.service')
                self.hareload = subprocess.check_call('systemctl reload haproxy.service', shell=True)
                self.logger.warning('The haproxy status : {:d} '.format(self.hareload))
                self.logger.warning('The haproxy reload success !')
            else:
                self.logger.warning('The haproxy status : {:d} '.format(self.hareload))
        except subprocess.CalledProcessError as err002:
            self.logger.error('Set haproxy.cfg Failed,error code {} !'.format(err002))

    def upgradeSlave(self, slaver_name):
        try:
            self.upgrade_slave = subprocess.check_call(
                "sed -i '/{}/s/weight 0/weight 10/g' /etc/haproxy/haproxy.cfg".format(slaver_name), shell=True)
            if self.degrade_slave == 0:
                self.logger.warning('Modify haproxy.cfg succeed, begin to reload haproxy.service')
                self.hareload = subprocess.check_call('systemctl reload haproxy.service', shell=True)
                self.logger.warning('The haproxy status : {:d} '.format(self.hareload))
                self.logger.warning('The haproxy reload success !')
            else:
                self.logger.error('The haproxy status : {:d} '.format(self.hareload))
        except subprocess.CalledProcessError as err005:
            self.logger.error('Set haproxy.cfg Failed,error code %s !' % err005)

    def orchookStart(self):
        self.hooksthread = threading.Timer(2, orchooks.infoFromOrc)
        self.hooksthread.start()


if __name__ == '__main__':
    orchooks = OrcHooks()

    try:
        orchooks.orchookStart()

    except Exception as err001:
        print(err001)
