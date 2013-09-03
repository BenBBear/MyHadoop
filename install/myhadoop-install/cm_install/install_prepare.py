# -*- coding=utf-8 -*-

import sys
from cm_conf.confs import *
from utils import *
import install

def change_cdh_dir_permission(root_pass, hosts=read_host_file()):
    """
    Make sure the CDH service have the permission to write her own file
    @return:
    """
    install_dirs = CDH_install_dir

    for h in hosts:
        ssh = ssh_connect(h, ssh_port, username, root_pass)
        tmp = install_dirs
        while not tmp == "":
            ssh_exc_cmd(ssh, 'chmod a+rx %s' % tmp)
            tmp = tmp[:tmp.rfind('/')]

def prepare_dirs(root_pass, hosts=read_host_file()):
    """
    begin install to create we will use the dirs and also this method will check the dir is exist.
    if the dir is exist install will exit.
    @return:
    """
    change_cdh_dir_permission(root_pass, hosts)

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
            if d == CDH_data_dir and 'log' in out :
                out.remove('log')

            if len(out) != 0: # the directory not empty
                if h not in err_host:
                    err_host.append(h)
                logInfo("The %s server the directory %s is not empty please check that and make sure the %s "
                        "directory is empty." % (h, d, d), color='red')

    if len(err_host) != 0:
        logInfo("Prepare install dir failed, In the %s servers some directory %s are not empty, %s"
                % (err_host, directorys, EXIT_MSG), color='red')

        sys.exit(-1)


def create_soft_links(root_pass, hosts=read_host_file()):
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

            sys.exit(-1)

        ssh_exc_cmd(ssh, 'ln -s %s /opt/cloudera' % CDH_install_dir)


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
    os.system("mkdir %s" % MYSQL_DATA_DIR)
    os.system("mkdir %s" % MYSQL_BINLOG_DIR)
    os.system("chown mysql:mysql %s" % MYSQL_BINLOG_DIR)

    logInfo("config the mysql for cloudera manager", color='green')

    binlog_conf = ""
    try:
        if is_os_gt5():
            binlog_conf = "binlog_format           = mixed"
        else:
            binlog_conf = ""
    except:
        pass

    mysql_conf = """
[mysqld]
transaction-isolation=READ-COMMITTED
datadir=%s
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
log-bin=%s/mysql_binary_log
# For MySQL version 5.1.8 or later. Comment out binlog_format for older versions.

%s

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
    """ % (MYSQL_DATA_DIR, MYSQL_BINLOG_DIR, binlog_conf)

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
        pass

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


def create_user(root_pass, hosts=read_host_file(), isadd=False):
    """
    create cm server user
    @return:
    """
    cmd6 = 'useradd -r --home=%s/run/cloudera-scm-server/  --no-create-home --shell=/bin/false --comment "Cloudera SCM User" cloudera-scm' % CMF_ROOT
    cmd5 = 'useradd -r --home=%s/run/cloudera-scm-server/ --shell=/bin/false --comment "Cloudera SCM User" cloudera-scm' % CMF_ROOT

    if not isadd:
        if is_os_gt5():
            os.system(cmd6)
        else:
            os.system(cmd5)

    err_hosts = []

    for h in hosts:
        if h == socket.gethostname():
            continue

        ssh = ssh_connect(h, ssh_port, username, root_pass, timeout_=10)
        if is_os_gt5():
            stdin, stdout, stderr = ssh.exec_command(cmd6)
        else:
            stdin, stdout, stderr = ssh.exec_command(cmd5)

        errors = stderr.readlines()

        ssh.close()

        if len(errors) > 0:
            err_hosts.append(h)

    if len(err_hosts) != 0:
        if is_os_gt5():
            logInfo('Create "cloudera-scm" user failed in %s hosts, you can create "cloudera-scm" user in %s hosts user "%s" command.' %(err_hosts, err_hosts, cmd6))
        else:
            logInfo('Create "cloudera-scm" user failed in %s hosts, you can create "cloudera-scm" user in %s hosts user "%s" command.' %(err_hosts, err_hosts, cmd5))
        sys.exit(-1)



def install_jdk(root_pass):
    """
    install jdk
    @return:
    """
    if not os.path.exists('/usr/java/%s' % jdk_unpack_name):
        if not os.path.exists('/usr/java'):
            os.mkdir('/usr/java')

        os.chdir('%s/tools' % install_root_dir)
        os.system('tar -zxf %s/tars/%s -C /usr/java' % (install_root_dir, jdk_tar_name))

    dispatch_jdk(root_pass)

def dispatch_jdk(root_pass, hosts=read_host_file()):
    """
    dispatch jdk to all server
    @return:
    """
    source = '%s/tars/%s' % (install_root_dir, jdk_tar_name)
    target = cm_install_dir + '/%s' % jdk_tar_name
    # os.chdir('/usr/java/')
    # os.system('tar -czf %s %s' % (source, jdk_unpack_name,))
    # os.chdir('/home')

    err_hosts = []
    for h in hosts:
        if h == socket.gethostname():
            continue
            # there use scp to dispatch the
        logInfo("dispatch the JDK to other servers and install it")
        try:
            sftp = get_sftp(h, ssh_port, username, root_pass)
            # if target exists then remove it first
            try:
                sftp.remove(target)
            except:
                pass
                # upload the file
            sftp.put(source, target)
            sftp.close()

            ssh = ssh_connect(h, ssh_port, username, root_pass, timeout_=30)
            try:
                ssh.exec_command('mkdir /usr/java')
                ssh.exec_command('rm -rf /usr/java/%s' % jdk_unpack_name)
            except:
                pass
            stdin, stdout, stderr = ssh.exec_command('tar zxf %s -C /usr/java' % (target,))
            for o in stdout.readlines():
                pass

            ssh.exec_command('chmod 755 /usr/java/%s/bin/*' % (jdk_unpack_name,))
            ssh.close()
        except Exception, ex:
            err_hosts.append(h)
            logInfo("Upload the file: %s to %s as %s failed. info is: %s " % (source, h, target, ex.message,),
                    color='red')

    if len(err_hosts) != 0:
        logInfo("Dispatch the JDK in %s hosts failed, %s " % (err_hosts, EXIT_MSG,), color='red')
        install.rollback_to_innit(root_pass)
        sys.exit(-1)