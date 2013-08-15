#!/bin/sh

# install the python third part tools

bin=`which $0`
bin=`dirname ${bin}`
bin=`cd "$bin"; pwd`

cd $bin/tools
tar zxf setuptools-0.9.8.tar.gz
cd setuptools-0.9.8
python setup.py install

cd ../
tar zxf paramiko-1.11.0.tar.gz
cd paramiko-1.11.0
easy_install ./

cd ../../

python myhadoop.py $*
