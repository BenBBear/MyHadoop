#!/usr/bin/env python

"""
 this script just set the oder step to install the Myhadoop
"""
from cm_install.check_env import *
from cm_install.install_prepare import *
from cm_install.install import *

def check_env():
    check_env()

def install():
    prepare_dirs()
    create_soft_links()
    install_mysql()
    create_user()

    unpack_cm()
    init_database()
    change_cnf()
    dispatch_cm()
    put_local_repo()
    start_cm_server()
    start_cm_agent()

if __name__ == '__main__':
    install()