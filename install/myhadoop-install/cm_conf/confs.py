# -*- coding=utf-8 -*-

# root password and ssh port. this is just for test. root password and ssh port should be
# input as the myhadoop.py args
ssh_port = 22

mysql_pass = '123456' # this password will set for mysql root user and for cloudera manager databases user.

# cm_tar is you put in tars of the CM tar package
cm_install_dir = '/home/hadoop/cloudera-manager'
cm_tar = 'cloudera-manager-el6-cm4.6.2_x86_64.tar.gz'
CMF_ROOT = '%s/cm-4.6.2' % cm_install_dir # cm-4.6.2 depends on the version of cm

""" CM connetct info  """
CM_HOST = '' # if this not config then will use the hostname of the script to run
CM_PORT = '7180'
CM_USERNAME = 'admin'
CM_USER_PASSWORD = 'admin'

# can use default
jdk_tar_name = 'jdk-7u3-linux-x64.tar.gz'
jdk_unpack_name = 'jdk1.7.0_03'
CDH_install_dir = '/home/hadoop/cloudera'
CDH_data_dir = '/home/hadoop/CDH-data'

MYSQL_DATA_DIR = '/home/hadoop/mysql-data'
MYSQL_BINLOG_DIR = '/home/hadoop/mysql-binlog'
