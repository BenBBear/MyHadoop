# -*- coding=utf-8 -*-

import sys
from cm_conf.confs import *
from utils import *
import install

def prepare_dirs(root_pass, hosts = read_host_file()):
    """
    begin install to create we will use the dirs and also this method will check the dir is exist.
    if the dir is exist install will exit.
    @return:
    """
    # hosts = read_host_file()
    err_host = []
    directorys = [
        cm_install_dir,
        CDH_data_dir,
        '%s/log' % CDH_data_dir,
        CDH_install_dir,
    ]
    for h in hosts:
        ssh = ssh_connect(h, ssh_port, username, root_pass)
        for d in directorys:
            ssh_exc_cmd(ssh, 'mkdir %s' % d)
            # ssh_exc_cmd(ssh, 'chmod 777 %s' % d)
            # check the dir is empty
            stdin, stdout, stderr = ssh_exc_cmd(ssh, 'ls %s' % d)
            out = stdout.readlines()
            if d == CDH_data_dir and 'log' in out > -1:
                out.remove('log')

            if len(out) != 0: # the directory not empty
                if h not in err_host:
                    err_host.append(h)
                logInfo("The %s server the directory %s is not empty please check that and make sure the %s "
                        "directory is empty." % (h, d, d), color='red')

    if len(err_host) != 0:
        logInfo("Prepare install dir failed, In the %s servers some directory %s are not empty, %s"
                % (err_host, directorys, EXIT_MSG), color='red')

        install.rollback_to_innit(root_pass)

        sys.exit(-1)


def create_soft_links(root_pass, hosts = read_host_file()):
    """
    Create soft links for CDH ln -s /home/cloudera /opt/
    @return:
    """
    # hosts = read_host_file()
    for h in hosts:
        ssh = ssh_connect(h, ssh_port, username, root_pass)
        # /opt/cloudera directory should be not exist
        stdin, stdout, stderr = ssh_exc_cmd(ssh, 'ls /opt/cloudera')
        err = stderr.readlines()
        if len(err) == 0:
            logInfo("The /opt/cloudera have exist now, before install Myhadoop /opt/cloudera directory should "
                    "have not exist. %s" % EXIT_MSG, color='red')

            install.rollback_to_innit(root_pass)

            sys.exit(-1)

        ssh_exc_cmd(ssh, 'ln -s %s /opt/' % CDH_install_dir)


def install_mysql(root_pass):
    """
    Install the mysql through yum and config the mysql for cloudera manager
    @return:
    """
    logInfo("install the mysql through the yum", color='green')
    os.system('yum -y install mysql-server')

    logInfo("install the mysql-connector-java ", color='green')
    os.system('yum -y install mysql-connector-java')

    #create dir
    os.system("mkdir /home/mysql-data")
    os.system("mkdir /home/mysql-binlog")
    os.system("chown mysql:mysql /home/mysql-binlog")

    logInfo("config the mysql for cloudera manager", color='green')
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
        logInfo("When config the mysql config file failed. and install will exit.", color='red')

        install.rollback_to_innit(root_pass)

        sys.exit(-1)

    # start mysql server
    os.system('/etc/init.d/mysqld start')

    # add password for the mysql root user
    ssh = ssh_connect('localhost', ssh_port, username, root_pass)
    stdin, stdout, stderr = ssh_exc_cmd(ssh, '/usr/bin/mysql_secure_installation')
    stdin.write("\n")
    stdin.flush()
    stdin.write("y\n")
    stdin.flush()
    stdin.write(mysql_pass + '\n')
    stdin.flush()
    stdin.write(mysql_pass + '\n')
    stdin.flush()
    stdin.write("y\n")
    stdin.flush()
    stdin.write("n\n")
    stdin.flush()
    stdin.write("n\n")
    stdin.flush()
    stdin.write("y\n")
    stdin.flush()
    stdin.write('\n\n\n\n')
    stdin.flush()

    for s in stdout.readlines():
        print s

    ssh.close()
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
    os.system('useradd --system --home=%s/run/cloudera-scm-server/  --no-create-home '
              '--shell=/bin/false --comment "Cloudera SCM User" cloudera-scm' % CMF_ROOT)

def dispatch_jdk(root_pass, hosts = read_host_file()):
    """
    dispatch jdk to all server
    @return:
    """
    source = '%s/tools/%s' % (install_root_dir, jdk_bin_name,)
    target = cm_install_dir + '/%s' % jdk_bin_name
    err_hosts = []
    for h in hosts:
        if h == socket.gethostname():
            continue
        # there use scp to dispatch the
        logInfo("dispatch the JDK to other servers and install it")
        try:
            sftp = get_sftp(h, ssh_port, username, root_pass)
            # if target exists then remove it first
            sftp.remove(target)
            # upload the file
            sftp.put(source, target)
            sftp.close()

            # unpack the target file
            ssh = ssh_connect(h, ssh_port, username, root_pass)
            ssh.exec_command('%s/tools' % install_root_dir)
            ssh.exec_command('sh %s' % (target,))
            ssh.exec_command('mkdir /usr/java')
            ssh.exec_command('rm -rf /usr/java/%s' % jdk_unpack_name) # if exist delete it first
            ssh.exec_command('mv %s/%s /usr/java/%s' % (cm_install_dir, jdk_unpack_name, jdk_unpack_name))
            ssh.close()
        except Exception, ex:
            err_hosts.append(h)
            logInfo("Upload the file: %s to %s as %s failed. info is: %s " % (source, h, target, ex.message,), color='red')

    if len(err_hosts) != 0:
        logInfo("Dispatch the JDK in %s hosts failed, %s " % (err_hosts, EXIT_MSG,), color='red')
        install.rollback_to_innit(root_pass)
        sys.exit(-1)

def install_jdk(root_pass):
    """
    install jdk
    @return:
    """
    os.chdir('%s/tools' % install_root_dir)
    os.system('sh %s/tools/%s' %(install_root_dir, jdk_bin_name))
    if not os.path.exists('/usr/java'):
        os.mkdir('/usr/java')
    os.system('mv %s/tools/%s /usr/java/%s' % (install_root_dir, jdk_unpack_name, jdk_unpack_name))

    dispatch_jdk(root_pass)