#!/bin/sh
# ssh port should be changed for your use
sshport=9922
root_password=123456

hostname_file=hosts_name.txt


save_keys()
{
  #echo "Setup public key $1";
  ID=`hostid`;
  PUB=$(cat "$HOME/.ssh/id_rsa.pub");
  expect -c "
    set timeout 1;
    spawn ssh -p $sshport root@$1;
    expect \"(yes/no)?\" {send \"yes\n\"};
    expect \"password:\" {send \"$2\n\"} \"$\" {};
    exec sleep 1;
    send \"mkdir -p ~/.ssh\n\";
    send \"touch ~/.ssh/authorized_keys\n\";
    send \"echo $PUB | cat - ~/.ssh/authorized_keys | sort | uniq > /tmp/secure-keys\n\";
    send \"mv -f /tmp/secure-keys ~/.ssh/authorized_keys\n\";
    send \"touch ~/.ssh/.ID$ID\n\";
    send \"chmod 700 ~/.ssh\n\";
    send \"chmod 600 ~/.ssh/authorized_keys\n\";
    exec sleep 1;
    send \"exit\n\";
    expect";
}

keygen()
{
  expect -c "
    spawn ssh-keygen -t rsa;
    expect \"save the key\";
    send \"\n\";
    expect \"empty for no passphrase\" {send \"\n\"} \"Overwrite (y/n)?\" {send \"y\n\"};
    expect \"same passphrase again:\";
    send \"\n\";
    expect"
}

setup_keys()
{
  if [ ! -r "$HOME/.ssh/id_rsa.pub" ]; then
    keygen;
  fi;

  for j in `cat ./$hostname_file`; do
    #echo "";
    #echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>";
    echo ">>>>>>>>>>>>>>>>>>> $j >>>>>>>>>>>>>>>>>>>>";
    #echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>";
    save_keys $j $root_password;
  done;
}
setup_keys

# check root user or has root permission
checkpassed=1
echo "begin to check the root permission....."
if [ `id -u` -ne 0 ]; then    
    echo "ERROR ################## You are not root user, Please re-run ${this_file} as root."
    checkpassed=0
else
    echo "*** root permission check passed!"    
fi 

#check the root ssh and system time synchronization
echo "begin to check the root ssh and system time synchronization.........."
for IP in `cat ./$hostname_file`
do
    ssh -p $sshport root@$IP "ntpdate cn.pool.ntp.org"
    if [ $? -eq 0 ]; then
        echo "*** check root ssh to $IP passed!"
    else
        echo "ERROR ################## root can't ssh to $IP. Please check!"
        checkpassed=0
    fi
done



#check the selinux
echo "begin to check the selinux is been shutdown....."
for IP in `cat ./$hostname_file`
do 
    seout=`ssh -p $sshport root@$IP "getenforce"`
    if [ $seout != "Disabled" ]; then
        echo "ERROR ################## $IP server the Selinux must be shutdown to install the cloudera-manager"
        checkpassed=0
    else
        echo "*** $IP server selinux has been shutdown, selinux echeck passed!"
    fi
done



#check the iptables
echo "begin to check the iptables status......"
for IP in `cat ./$hostname_file`
do 
    ssh -p $sshport root@$IP "/sbin/service iptables status 1>/dev/null 2>&1"
    if [ $? -ne 0 ]; then
        echo "*** $IP server iptables has been shutdown, iptables echeck passed!"
    else
        echo "ERROR ################## $IP server: The iptables must be shutdown to install the cloudera-manager, when install finished you can open it and set iptables rule for the service port."
        checkpassed=0
    fi
done

# check the hostname 
echo "begin to check the hostname setting.........."
for IP in `cat ./$hostname_file`
do 
    h1=`ssh -p $sshport root@$IP "cat /etc/sysconfig/network | grep HOSTNAME " | awk -F '=' '{print $2}'`
    if [ `ssh -p $sshport root@$IP "hostname"` != $h1 ]; then
        echo "ERROR ################## $IP server The hostname are not consistent, Please check that!"
        checkpassed=0
    else
        echo "*** $IP server hostname check passed!"
    fi
done


# check the yum
echo "begin to check the yum....."
for IP in `cat ./$hostname_file`
do 
    ssh -p $sshport root@$IP "yum install -y vim 1>/dev/null"
    if [ $? -eq 0 ]; then
        echo "*** $IP server yum can install software, yum echeck passed!"
    else
        echo "ERROR ################## $IP server The Yum seens can't install the software. Please check that, becase cloudera-manager will throud yum install some package."
        checkpassed=0
    fi
done


# check the system disk space if less than 8G
echo "begin to check the system disk space......"
for IP in `cat ./$hostname_file`
do 
    space=`ssh -p $sshport root@$IP "df -h /" | awk '{print $4}'| grep G | awk '{printf("%d", $1)}'`
    if [ $space -gt 8 ]; then
        echo "***** $IP server disk space check passed!"
    else
        echo "warnning************ $IP server The system disk space is less than 8G aviable. the space is: $space" 
        checkpassed=0
    fi
done

# if all check have passed then download the cloudera manager installer and install it.
if [ $checkpassed -ne 0 ]; then
    echo "begin to install cloudera-manager......"
    # download the cloudera-manager-installer.bin
    echo "download the cloudera-manager-installer.bin for install"
    wget http://archive.cloudera.com/cm4/installer/latest/cloudera-manager-installer.bin

    # install cloudera-manager
    echo "begin to install the cloudera-manager"
    chmod a+x cloudera-manager-installer.bin
    ./cloudera-manager-installer.bin
else
    echo "The env requirement check failed, nothing installed. Please fixed it and re-run the install sh!"
fi