# -*- coding=utf-8 -*-

import sys
from cm_conf.confs import *
from utils import *

def prepare_dirs():
    """
    begin install to create we will use the dirs and also this method will check the dir is exist.
    if the dir is exist install will exit.
    @return:
    """
    hosts = read_host_file()
    err_host = []
    directorys = [
        '/home/cloudera-manager',
        '/home/CDH-data',
        '/home/CDH-data/log',
        '/home/cloudera',
    ]
    for h in hosts:
        ssh = ssh_connect(h, ssh_port, username, root_pass)
        for d in directorys:
            ssh_exc_cmd(ssh, 'mkdir %s' % d)
            # ssh_exc_cmd(ssh, 'chmod 777 %s' % d)
            # check the dir is empty
            stdin, stdout, stderr = ssh_exc_cmd(ssh, 'ls %s' % d)
            out = stdout.readlines()
            if d == '/home/CDH-data' and 'log' in out > -1:
                out.remove('log')

            if len(out) != 0: # the directory not empty
                err_host.append(h)
                logInfo("The %s server the directory %s is not empty please check that and make sure the %s "
                        "directory is empty." % (h, d, d))

    if len(err_host) != 0:
        logInfo("Prepare install dir failed, In the %s servers some directory %s are not empty, %s"
                % (err_host, directorys, EXIT_MSG))
        sys.exit(-1)


def create_soft_links():
    """
    Create soft links for CDH ln -s /home/cloudera /opt/
    @return:
    """
    hosts = read_host_file()
    err_host = []
    for h in hosts:
        ssh = ssh_connect(h, ssh_port, username, root_pass)
        # /opt/cloudera directory should be not exist
        stdin, stdout, stderr = ssh_exc_cmd(ssh, 'ls /opt/cloudera')
        err = stderr.readlines()
        if len(err) == 0:
            logInfo("The /opt/cloudera have exist now, before install Myhadoop /opt/cloudera directory should "
                    "have not exist. %s" % EXIT_MSG)
            sys.exit(-1)
        ssh_exc_cmd(ssh, 'ln -s /home/cloudera /opt/')


def install_mysql():
    """
    Install the mysql through yum and config the mysql for cloudera manager
    @return:
    """
    logInfo("install the mysql through the yum")
    os.system('yum -y install mysql-server')

    logInfo("install the mysql-connector-java ")
    os.system('yum -y install mysql-connector-java')

    #create dir
    os.system("mkdir /home/mysql-data")
    os.system("mkdir /home/mysql-binlog")
    os.system("chown mysql:mysql /home/mysql-binlog")

    logInfo("config the mysql for cloudera manager")
    mysql_conf = """
[mysqld]
transaction-isolation=READ-COMMITTED
datadir=/home/mysql-data
socket=/var/lib/mysql/mysql.sock
user=mysql
# Disabling symbolic-links is recommended to prevent assorted security risks
# symbolic-links=0

key_buffer              = 16M
key_buffer_size         = 32M
max_allowed_packet      = 16M
thread_stack            = 256K
thread_cache_size       = 64
query_cache_limit       = 8M
query_cache_size        = 64M
query_cache_type        = 1
# Important: see Configuring the Databases and Setting max_connections
max_connections         = 200

# log-bin should be on a disk with enough free space
log-bin=/home/mysql-binlog/mysql_binary_log
# For MySQL version 5.1.8 or later. Comment out binlog_format for older versions.
binlog_format           = mixed

read_buffer_size = 2M
read_rnd_buffer_size = 16M
sort_buffer_size = 8M
join_buffer_size = 8M

# InnoDB settings
innodb_file_per_table = 1
innodb_flush_log_at_trx_commit  = 2
innodb_log_buffer_size          = 64M
innodb_buffer_pool_size         = 1G
innodb_thread_concurrency       = 8
innodb_flush_method             = O_DIRECT
innodb_log_file_size = 512M

[mysqld_safe]
log-error=/var/log/mysqld.log
pid-file=/var/run/mysqld/mysqld.pid
    """
    os.system("mv /etc/my.cnf /etc/my.cnf.bak")
    try:
        f = file('/etc/my.cnf', 'w')
        f.write(mysql_conf)
        f.close()
    except Exception, ex:
        logInfo("When config the mysql config file failed. and install will exit.")
        sys.exit(-1)

    # start mysql server
    os.system('/etc/init.d/mysqld start')

    # add password for the mysql root user todo
    ssh = ssh_connect('localhost', ssh_port, username, root_pass)
    stdin, stdout, stderr = ssh_exc_cmd(ssh, '/usr/bin/mysql_secure_installation')
    stdin.write("")
    stdin.write("y")
    stdin.write(mysql_pass)
    stdin.write(mysql_pass)
    stdin.write("y")
    stdin.write("n")
    stdin.write("n")
    stdin.write("y")
    for s in stdout.readlines():
        print s


    #os.system('/usr/bin/mysql_secure_installation')

    # add start up by system
    os.system('/sbin/chkconfig mysqld on')
    os.system('/sbin/chkconfig --list mysqld')

    # create database for cloudera manager

    # create Activity Monitoror database
    os.system('mysql -uroot -p%s -e "create database amon DEFAULT CHARACTER SET utf8;"' % (mysql_pass))
    os.system('mysql -uroot -p%s -e "grant all on amon.* TO \'amon\'@\'%s\' IDENTIFIED BY \'%s\';"' % (mysql_pass,
                                                                                                       hs, mysql_pass))
    # create Service Monitor database
    os.system('mysql -uroot -p%s -e "create database smon DEFAULT CHARACTER SET utf8;"' % (mysql_pass))
    os.system('mysql -uroot -p%s -e "grant all on smon.* TO \'smon\'@\'%s\' IDENTIFIED BY \'%s\'"' % (mysql_pass, hs,
                                                                                                      mysql_pass))
    # create Report Manager database
    os.system('mysql -uroot -p%s -e "create database rman DEFAULT CHARACTER SET utf8;"' % (mysql_pass))
    os.system('mysql -uroot -p%s -e "grant all on rman.* TO \'rman\'@\'%s\' IDENTIFIED BY \'%s\';"' % (mysql_pass, hs,
                                                                                                       mysql_pass))
    # create Host Monitor database
    os.system('mysql -uroot -p%s -e "create database hmon DEFAULT CHARACTER SET utf8;"' % (mysql_pass))
    os.system('mysql -uroot -p%s -e "grant all on hmon.* TO \'hmon\'@\'%s\' IDENTIFIED BY \'%s\';"' % (mysql_pass, hs,
                                                                                                       mysql_pass))
    # create Cloudera Navigator database
    os.system('mysql -uroot -p%s -e "create database nav DEFAULT CHARACTER SET utf8;"' % (mysql_pass))
    os.system('mysql -uroot -p%s -e "grant all on nav.* TO \'nav\'@\'%s\' IDENTIFIED BY \'%s\'"' % (mysql_pass, hs,
                                                                                                    mysql_pass))
    # create Hive metastore database
    os.system('mysql -uroot -p%s -e "create database hive DEFAULT CHARACTER SET utf8;"' % (mysql_pass))
    os.system('mysql -uroot -p%s -e "grant all on hive.* TO \'hive\'@\'%s\' IDENTIFIED BY \'%s\';"' % (mysql_pass, hs,
                                                                                                       mysql_pass))


def create_user():
    """
    create cm server user
    @return:
    """
    os.system('useradd --system --home=/home/cloudera-manager/cm-4.6.2/run/cloudera-scm-server/  --no-create-home '
              '--shell=/bin/false --comment "Cloudera SCM User" cloudera-scm')

