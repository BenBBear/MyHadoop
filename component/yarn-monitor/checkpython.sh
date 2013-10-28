#!/bin/bash
date
num=`ps aux|grep python|grep collect|wc -l`
if [ $num -ge 6 ] ;then
	echo $num
	ps aux|grep python|grep collect|awk '{print $2}'|xargs kill
else
	echo "python number is ok"
fi;
