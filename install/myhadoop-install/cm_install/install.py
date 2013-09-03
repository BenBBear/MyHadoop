# -*- coding=utf-8 -*-

from utils  import *
from cm_conf.confs import *
import os, sys
import utils



def unpack_cm():
    """
    unpack the cm software to /home/cloudera-manager
    @return:
    """
    os.system('tar -zxf %s/tars/%s -C %s' % (install_root_dir, cm_tar, cm_install_dir))

def init_database():
    """
    init cm server database
    @return:
    """
    os.system('mysql -uroot -p%s -e "grant all on *.* TO \'temp\'@\'%s\' IDENTIFIED BY \'123456\' with grant option;"'
              % (mysql_pass, hs))
    os.system('mysql -uroot -p%s -e "grant all on scm.* TO \'scm\'@\'%s\' IDENTIFIED BY \'scm\' with grant option;"'
              % (mysql_pass, hs))
    os.system('%s/share/cmf/schema/scm_prepare_database.sh mysql -h %s -P3306 -u temp -p123456 --scm-host '
              '%s scm scm scm' % (CMF_ROOT, LOCL_HOST, LOCL_HOST))
    os.system('mysql -uroot -p%s -e "drop user \'temp\'@\'%s\';"' % (mysql_pass, hs))

def change_cnf(root_pass):
    """
    change some conf of cm, special for cm agent
    @return:
    """
    # sed -i "s/server_host=localhost/server_host=`hostname`/g" $CMF_ROOT/etc/cloudera-scm-agent/config.ini
    conf_file = CMF_ROOT + os.path.sep + 'etc/cloudera-scm-agent/config.ini'
    os.system('sed -i "s/server_host=localhost/server_host=%s/g" %s' %(socket.gethostname(), conf_file))

    # and JAVA_HOME and CMF_ROOT to $CMF_ROOT/etc/default/cloudera-scm-server %CMF_ROOT/etc/default/cloudera-scm-agent
    # JAVA_HOME just set for cm server is ok and agent will to locate the java home in /usr/java/jdk1.6*
    java_home_str = 'export JAVA_HOME=/usr/java/%s' % jdk_unpack_name
    cmf_root = 'export CMF_ROOT=%s' % CMF_ROOT
    cm_server_f = '%s/etc/default/cloudera-scm-server' % CMF_ROOT
    cm_agent_f = '%s/etc/default/cloudera-scm-agent' % CMF_ROOT
    if os.path.exists('/usr/java/%s' % jdk_unpack_name):
        os.system("sed -i '1 i\%s' %s" % (java_home_str, cm_server_f))
        os.system("sed -i '1 i\%s' %s" % (java_home_str, cm_agent_f))
    else:
        logInfo("Can't find the JDK, %s" % EXIT_MSG, color='red')

        rollback_to_innit(root_pass)
        sys.exit(-1)
    os.system("sed -i '1 i\%s' %s" % (cmf_root, cm_server_f))
    os.system("sed -i '1 i\%s' %s" % (cmf_root, cm_agent_f))

    # and CMF_DEFAULTS to $CMF_ROOT/etc/init.d/cloudera-scm-server %CMF_ROOT/etc/init.d/cloudera-scm-agent
    CMF_DEFAULTS = 'export CMF_DEFAULTS=%s/etc/default' %CMF_ROOT
    cm_server_f = '%s/etc/init.d/cloudera-scm-server' % CMF_ROOT
    cm_agent_f = '%s/etc/init.d/cloudera-scm-agent' % CMF_ROOT
    os.system("sed -i '37 i\%s' %s" % (CMF_DEFAULTS, cm_server_f))
    os.system("sed -i '37 i\%s' %s" % (CMF_DEFAULTS, cm_agent_f))


def dispatch_cm(root_pass, hosts = read_host_file()):
    """
    dispatch the cm to all the server you want to install the  cm agent
    @return:
    """
    # because use scp to dispatch the file will ack to input the password, so there we pack the agent and use
    # paramiko to dispatch and unpack the file to dist
    source = cm_install_dir + '/cloudera-manager-myhadoop.tar.gz'
    target = cm_install_dir + '/cloudera-manager-myhadoop.tar.gz'
    os.chdir(cm_install_dir)

    if not os.path.exists(source):
        os.system('tar -czf %s ./' % source)

    os.chdir(cm_install_dir)
    err_hosts = []
    for h in hosts:
        if h == socket.gethostname():
            continue
        # there use scp to dispatch the
        logInfo("dispatch the cm agent to other servers")
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

            # unpack the target file
            ssh = ssh_connect(h, ssh_port, username, root_pass)
            stdin, stdout, stderr = ssh.exec_command('tar -zxf %s -C %s' % (target, cm_install_dir,))

            for o in stdout.readlines():
                pass
                #logInfo(o)

            ssh.close()
        except Exception, ex:
            err_hosts.append(h)
            logInfo("Upload the file: %s to %s as %s failed. info is: %s " % (source, h, target, ex.message,),
                    color='red')

    if len(err_hosts) != 0:
        logInfo("Dispatch the CM agent in %s hosts failed, %s " % (err_hosts, EXIT_MSG,), color='red')
        rollback_to_innit(root_pass)
        sys.exit(-1)

def put_local_repo():
    """
    put local parcels file to the /opt/cloudera/parcel-repo
    @return:
    """
    target = '/opt/cloudera/parcel-repo'
    os.mkdir(target)
    os.system('chown cloudera-scm:cloudera-scm %s' % target)

    parcels_dir = install_root_dir +  '/parcels'
    os.system('mv %s/* %s' % (parcels_dir, target,))


def start_cm_server():
    """
    start the cm server
    @return:
    """
    # lohost = socket.gethostname()
    # ssh = ssh_connect(lohost, ssh_port, username, root_pass)
    # stdin, stdout, stderr = ssh.exec_command('%s/cm-4.6.2/etc/init.d/cloudera-scm-server start' % cm_install_dir)
    # ssh.close()
    # errs = stderr.readlines()
    #
    # for o in stdout.readlines():
    #     logInfo(o)
    #
    # if len(errs) == 0:
    #     for err in errs:
    #         logInfo(err)
    #     logInfo("CM Server started, Now you can login http://%s:7180 to manager your CDH cluster." % lohost)
    # else:
    #     logInfo("CM Server start failed. %s" % EXIT_MSG)
    #     sys.exit(-1)

    # use paramiko can't start the server. os here to use os.system
    os.system('%s/etc/init.d/cloudera-scm-server start' % CMF_ROOT)
    logInfo("CM Server started, Now you can login http://%s:7180 to manager your CDH cluster." % socket.gethostname(), color='green')

def start_cm_agent(root_pass, hosts = read_host_file()):
    """
    start all cm agent
    @return:
    """
    err_hosts = []
    for h in hosts:
        ssh = ssh_connect(h, ssh_port, username, root_pass)
        ssh.exec_command('chmod a+x %s/etc/init.d/cloudera-scm-agent' % CMF_ROOT)
        stdin, stdout, stderr = ssh.exec_command('%s/etc/init.d/cloudera-scm-agent start' % CMF_ROOT)

        errors = stderr.readlines()
        if len(errors) == 0:
            for o in stdout.readlines():
                logInfo(o)
            logInfo("CM agent in %s server started." % h , color='green')
        else:
            for e in errors:
                logInfo(e, color='red')
            err_hosts.append(h)
            logInfo("CM agent in %s server start failed. %s" % (h,EXIT_MSG), color='red')

        ssh.close()

    if len(err_hosts) != 0:
        logInfo("The CM agent of the servers: %s start failed, please check that." % (err_hosts), color='red')

    logInfo("Install CM finished.", color='green')


def add_startup_on_init(root_pass, hosts = read_host_file(), isaddnode=False):
    """
    add cm start up by system
    @return:
    """
    if not isaddnode:
        os.system('rm -rf /etc/init.d/cloudera-scm-server')
        os.system('rm -rf /etc/rc.d/init.d/cloudera-scm-server')
        os.system('rm -rf /usr/sbin/cloudera-scm-server')
        os.system('ln -s %s/etc/init.d/cloudera-scm-server /etc/init.d/' % CMF_ROOT)
        os.system('ln -s %s/sbin/cmf-server /usr/sbin' % CMF_ROOT)
        os.system('/sbin/chkconfig cloudera-scm-server on')
        os.system('/sbin/chkconfig --list cloudera-scm-server')
    for h in hosts:
        ssh = ssh_connect(h, ssh_port, username, root_pass)
        ssh.exec_command('rm -rf /etc/init.d/cloudera-scm-agent')
        ssh.exec_command('rm -rf /usr/sbin/cloudera-scm-agent')
        ssh.exec_command('ln -s %s/etc/init.d/cloudera-scm-agent /etc/init.d/' % CMF_ROOT)
        ssh.exec_command('ln -s %s/sbin/cmf-agent /usr/sbin' % CMF_ROOT)
        ssh.exec_command('/sbin/chkconfig cloudera-scm-agent on')
        ssh.exec_command('/sbin/chkconfig --list cloudera-scm-agent')

def rollback_to_innit(root_pass, hosts = read_host_file()):
    """
    Stop the mysql service and delete the dirs we create
    @param root_pass:
    @param hosts:
    @return:
    """
    logInfo("begin rollback for failed servers: %s, " % hosts, color='red')
    os.system('/etc/init.d/mysqld stop')

    target = '/opt/cloudera/parcel-repo'
    parcels_dir = install_root_dir +  '/parcels/'
    os.system('mv %s/* %s' % (target, parcels_dir,))

    directorys = [
        cm_install_dir,
        CDH_data_dir,
        CDH_install_dir,
        MYSQL_DATA_DIR,
        MYSQL_BINLOG_DIR
    ]

    for h in hosts:
        ssh = ssh_connect(h, ssh_port, username, root_pass)

        for d in directorys:
            ssh.exec_command('rm -rf %s' % d)
        ssh.exec_command('rm -f /opt/cloudera')

        ssh.close()