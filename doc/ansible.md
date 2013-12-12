# Ansible

Ansible快速上手：<https://linuxtoy.org/archives/hands-on-with-ansible.html>

## 快速安装（CentOS 6.4）（要求Python 2.6+）

    wget https://bitbucket.org/pypa/setuptools/raw/bootstrap/ez_setup.py -O -|python -
    easy_install pip
    yum install python-devel
    pip install ansible
    which ansible


## 配置

/etc/ansible/hosts

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