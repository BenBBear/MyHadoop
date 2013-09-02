#!/usr/bin/env python

"""
 this script just set the oder step to install the Myhadoop
"""
from cm_install.check_env import *
from cm_install.install_prepare import *
from cm_install.install import *

import sys

def check_myhadoop_env(root_pass):
    check_env(root_pass)

def install_cm(root_pass):
    prepare_dirs(root_pass)
    create_soft_links(root_pass)
    install_mysql(root_pass)
    install_jdk(root_pass)
    create_user(root_pass)

    unpack_cm()
    init_database()
    change_cnf(root_pass)
    dispatch_cm(root_pass)
    put_local_repo()
    start_cm_server()
    start_cm_agent(root_pass)
    add_startup_on_init(root_pass)

if __name__ == '__main__':
    root_pass = str(sys.argv[1]) # set root password

    if is_os_gt5():
        os.system('easy_install termcolor')

    check_myhadoop_env(root_pass)
    install_cm(root_pass)