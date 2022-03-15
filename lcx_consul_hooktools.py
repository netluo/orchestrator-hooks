#!/usr/bin/env python3
import subprocess
import argparse


class OrcHookTools(object):
    def __init__(self):

        self.cls_ins = []
        self.ins_info = []

        self.cls_master: set = set()
        self.cls_name = []

        self.consul_token = ''

    def getClsName(self):
        self.cls_name.clear()
        self.cls_name = subprocess.getoutput(
            'orchestrator-client -c clusters').split('\n')
        print(self.cls_name)

    def getServerStat(self):
        self.getClsName()
        self.cls_ins.clear()
        self.ins_info.clear()
        for clsname in self.cls_name:
            server_status = subprocess.getoutput(
                'orchestrator-client -c topology -i {}'.format(clsname))
            for line in server_status.split('\n'):
                line = line.strip().strip('+').strip('-').strip()
                print(line)
                self.cls_ins.append(line.split(' ')[0])
                self.ins_info.append(line.split(
                    ' ')[-1].lstrip('[').rstrip(']').split(','))

    def getMasters(self):
        self.getServerStat()
        self.cls_master.clear()
        print(self.cls_ins)
        for instance in self.cls_ins:
            _master_host = subprocess.getoutput(
                'orchestrator-client -c which-master -i {}'.format(instance))
            if _master_host == ':0':
                self.cls_master.add(instance)

    def setWeight(self):
        self.getMasters()
        for i in range(len(self.cls_ins)):
            ins_name = self.cls_ins[i]

            if ins_name in self.cls_master:
                print("{} is Master, set weight to 0".format(ins_name))
                set_weight_status = subprocess.getoutput(
                    'consul kv put {} {}/weight 0'.format(self.consul_token, ins_name))
                print('set {} to weight 0'.format(ins_name), set_weight_status)

            else:
                if self.ins_info[i][1] == 'lag':
                    print('{} is Slaver, and the server is delay server,set weight to 0 !'.format(ins_name))
                    set_weight_status = subprocess.getoutput(
                        'consul kv put {} {}/weight 0'.format(self.consul_token, ins_name))
                    print('set {} to weight 0'.format(ins_name), set_weight_status)
                else:
                    print('{} is Slaver, set weight to 10'.format(ins_name))
                    set_weight_status = subprocess.getoutput(
                        'consul kv put {} {}/weight 10'.format(self.consul_token, ins_name))
                    print('set {} to weight 10'.format(ins_name), set_weight_status)

    def printWeight(self):
        for ins_name in self.cls_ins:
            ins_weight = subprocess.getoutput(
                'consul kv get {} -recurse {}/weight '.format(self.consul_token, ins_name))
            print(ins_weight)

    # def setHaproxyTmpl(self):


if __name__ == '__main__':
    hooktools = OrcHookTools()
    parse = argparse.ArgumentParser(description="Orchestrator Hooks Tools.", add_help=True)
    parse.add_argument('--set-consul-kv', help='set cluster info to consul(kv)', action='store_true', default=False)

    parse.add_argument('--set-ha-template', help='set haproxy config to consul-template', action='store_true',
                       default=False)
    parse.add_argument('--version', '-v', help='print version', action='store_true', default=False)
    parse.add_argument('--filename', '-f', help='the haproxy template filename', default=False,
                       type=argparse.FileType('a+'))
    parse.add_argument('--consul-token', '-t', help='set consul token if token is needed',
                       default=False)
    args = parse.parse_args()
    if args.version:
        print('1.1.1')
        print(args)
    if args.consul_token:
        hooktools.consul_token = '-token ' + str(args.consul_token)
    if args.set_consul_kv:
        hooktools.setWeight()
        hooktools.printWeight()
