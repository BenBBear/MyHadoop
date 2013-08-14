# -*- coding=utf-8 -*-

from utils  import *
from cm_conf.confs import *
import os, sys
import utils

cm_install_dir = '/home/cloudera-manager'

def unpack_cm():
    """
    unpack the cm software to /home/cloudera-manager
    @return:
    """
    tars_dir = os.path.abspath(os.path.dirname(__file__)) +  '/../tars'
    os.system('tar zxf %s/%s -C %s' % (tars_dir, cm_tar, cm_install_dir))

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
              '%s scm scm scm' % (CMF_HOME, LOCL_HOST, LOCL_HOST))
    os.system('mysql -uroot -p%s -e "drop user \'temp\'@\'%s\';"' % (mysql_pass, hs))

def change_cnf():
    """
    change some conf of cm, special for cm agent
    @return:
    """
    # sed -i "s/server_host=localhost/server_host=`hostname`/g" $CM_HOME/etc/cloudera-scm-agent/config.ini
    conf_file = CMF_HOME + os.path.sep + 'etc/cloudera-scm-agent/config.ini'
    os.system('sed -i "s/server_host=localhost/server_host=%s/g" %s' %(socket.gethostname(), conf_file))

def dispatch_cm():
    """
    dispatch the cm to all the server you want to install the  cm agent
    @return:
    """
    # because use scp to dispatch the file will ack to input the password, so there we pack the agent and use
    # paramiko to dispatch and unpack the file to dist
    source = '/home/cloudera-manager-myhadoop.tar.gz'
    target = cm_install_dir + '/cloudera-manager-myhadoop.tar.gz'
    os.chdir(cm_install_dir)
    os.system('tar -czf %s ./' % source)
    os.chdir('/home')
    err_hosts = []
    for h in read_host_file():
        if h == socket.gethostname():
            continue
        # there use scp to dispatch the
        logInfo("dispatch the cm agent to other servers")
        try:
            sftp = get_sftp(h, ssh_port, username, root_pass)
            # if target exists then remove it first
            sftp.remove(target)
            # upload the file
            sftp.put(source, target)
            sftp.close()

            # unpack the target file
            ssh = ssh_connect(h, ssh_port, username, root_pass)
            ssh.exec_command('tar zxf %s -C %s' % (target, cm_install_dir,))
            ssh.close()
        except Exception, ex:
            err_hosts.append(h)
            logInfo("Upload the file: %s to %s as %s failed. info is: %s " % (source, h, target, ex.message,))

    if len(err_hosts) != 0:
        logInfo("Dispatch the CM agent in %s hosts failed, %s " % (err_hosts, EXIT_MSG,))
        sys.exit(-1)

def put_local_repo():
    """
    put local parcels file to the /opt/cloudera/parcel-repo
    @return:
    """
    target = '/opt/cloudera/parcel-repo'
    os.mkdir(target)
    os.system('chown cloudera-scm:cloudera-scm %s' % target)

    parcels_dir = os.path.abspath(os.path.dirname(utils.__file__)) +  '/parcels'
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
    # if len(errs) == 0:
    #     for err in errs:
    #         logInfo(err)
    #     logInfo("CM Server started, Now you can login http://%s:7180 to manager your CDH cluster." % lohost)
    # else:
    #     logInfo("CM Server start failed. %s" % EXIT_MSG)
    #     sys.exit(-1)
    os.system('%s/cm-4.6.2/etc/init.d/cloudera-scm-server start' % cm_install_dir)

def start_cm_agent():
    """
    start all cm agent
    @return:
    """
    err_hosts = []
    for h in read_host_file():
        ssh = ssh_connect(h, ssh_port, username, root_pass)
        stdin, stdout, stderr = ssh.exec_command('%s/cm-4.6.2/etc/init.d/cloudera-scm-agent start' % cm_install_dir)

        if len(stderr.readlines()) == 0:
            logInfo("CM agent in %s server started." % h)
        else:
            err_hosts.append(h)
            logInfo("CM agent in %s server start failed. %s" % h)

        ssh.close()

    if len(err_hosts) != 0:
        logInfo("The CM agent of the servers: %s start failed, please check that.")

    logInfo("Install CM finished.")