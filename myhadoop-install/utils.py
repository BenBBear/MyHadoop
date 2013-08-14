
import os
import paramiko
import socket

EXIT_MSG = "so myhadoop install will exit. Please fix it and install again."
username = 'root'

CMF_HOME = "/home/cloudera-manager/cm-4.6.2"
LOCL_HOST= socket.gethostname()

hs = "%"

hosts_list = []

def read_host_file(filename='hosts_name.txt'):
    """
    Read the host file. Return a list of hostname.
    """
    if len(hosts_list) == 0:
        path = os.path.abspath(__file__) + os.path.sep + filename

        if (not (os.path.exists(path) or os.path.isfile(path))):
            raise Exception("The hosts_name file: %s you specific is not exist or this is a directory." %
                          path)

        for l in file(path).xreadlines():
            hostname = l.strip()
            if hostname:
                hosts_list.append(hostname)

    return hosts_list

def ssh_connect(host, port, username, password, timeout_=5):
    """
    connect to the host use ssh and return the ssh object
    @param ip:
    @param port:
    @param username:
    @param password:
    @param timeout:
    @return:
    """
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(host, port, username, password, timeout=timeout_)
        return ssh
    except Exception, ex:
        raise ex

def logInfo(msg):
    """
    print the info for user
    @param msg:
    @return:
    """
    print msg

def ssh_exc_cmd(ssh, cmd):
    return ssh.exec_command(cmd)



def get_sftp(host, port, username, password):
    try:
        t = paramiko.Transport((host, port))
        t.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(t)
        return sftp
    except Exception, ex:
        raise ex