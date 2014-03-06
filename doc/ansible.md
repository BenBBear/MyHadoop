# Ansible

Ansible快速上手：<https://linuxtoy.org/archives/hands-on-with-ansible.html>

## 快速安装（CentOS 6.4）（要求Python 2.6+）

    wget --no-check-certificate https://bitbucket.org/pypa/setuptools/raw/bootstrap/ez_setup.py -O -|python - --insecure
    # 如果下载中报SSL等错误，通常是你的机器没有安装openssl，yum安装它
    easy_install pip
    yum install python-devel
    pip install simplejson
    pip install ansible
    which ansible

注意，远程被控制端的python也需要安装`pip install simplejson`

 wget --no-check-certificate https://fedoraproject.org/static/217521F6.txt -O RPM-GPG-KEY-EPEL


## 配置

默认机器Host配置在`/etc/ansible/hosts`,或者使用`-i`参数指定位置。

[webservers]
asdf.example.com  ansible_ssh_port=5000   ansible_ssh_user=alice
jkl.example.com   ansible_ssh_port=5001   ansible_ssh_user=bob

[testcluster]
localhost           ansible_connection=local
/path/to/chroot1    ansible_connection=chroot
foo.example.com
bar.example.com

ansible_python_interpreter=/usr/bin/python2.4

ansible webservers -a "echo ok"
ansible cnode431:9922 -a "echo ok" -k   #每有免密码时

## 模块

<http://www.ansibleworks.com/docs/modules.html>

*   ping
*   copy
*   command

## 使用示例

    ansible gateway -m copy -a " src=/home/hadoop/hive-auth-hook.jar dest=/opt/cloudera/parcels/CDH/lib/hive/lib/hive-auth-hook.jar owner=root group=root mode=0755" --sudo

 
