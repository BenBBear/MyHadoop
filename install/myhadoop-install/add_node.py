#!/usr/bin/env python

usage = """
This script is for new machine to add to cluster, the args is the hostname1 hostname2 ...

usage:  python add_node.py root_password hostname1 hostname2 or

        python add_node.py root_password

        and config the hostnames
"""

hostnames = [

]

import sys
from cm_install.check_env import *
from cm_install.install_prepare import *
from cm_install.install import *

def check_all_env(root_pass, hosts):
    root_ssh_check(root_pass, hosts=hosts)
    selinux_check(root_pass, hosts=hosts)
    iptables_check(root_pass, hosts=hosts)
    hosts_check(root_pass, hosts=hosts)
    check_yum(root_pass, hosts=hosts)
    #syn_sys_time(root_pass, hosts=hosts)

def install_pre(root_pass, hosts):
    prepare_dirs(root_pass, hosts=hosts)
    create_soft_links(root_pass, hosts=hosts)
    dispatch_jdk(root_pass, hosts=hosts)
    create_user(root_pass, isadd=True)

def install_cm_agent(root_pass, hosts):
    dispatch_cm(root_pass, hosts=hosts)
    start_cm_agent(root_pass, hosts=hosts)
    add_startup_on_init(root_pass, hosts=hosts, isaddnode=True)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        logInfo(usage, color='red')
        sys.exit(-1)
    root_pass = sys.argv[1]

    if len(sys.argv) > 2:

        hostnames = sys.argv[2:]


    check_all_env(root_pass, hostnames)
    install_pre(root_pass, hostnames)
    install_cm_agent(root_pass, hostnames)
