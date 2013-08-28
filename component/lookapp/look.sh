#!/usr/bin/env bash
nmport=59842
host="mob616:50088"
host="platform30:8088"
version="v1"
accept="Accept: application/json"
resource="apps?state=RUNNING"

url="http://$host/ws/${version}/cluster/${resource}"
echo $url

dt=`date +"%Y%m%d%H%M%S"`
day=${dt::8}
dir=./data/$day
mkdir -p $dir
file=$dir/${dt}.json.log

curl -H "$accept" -X GET "$url" -o $file 2>/dev/null 

#http://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html
#http://mob616:50088/ws/v1/cluster/metrics
#http://mob616:50088/ws/v1/cluster/scheduler
#http://mob616:50088/ws/v1/cluster/apps
#http://mob616:50088/ws/v1/cluster/apps/{appid}
#http://mob616:50088/ws/v1/cluster/apps?state=RUNNING

#采集node manager的container的列表
dt=`date +"%Y%m%d%H%M"`
nodes=`cat ${HADOOP_CONF_DIR}/slaves`
for node in $nodes
do
    echo $node
    dir=./nodedata/$day/$node
    mkdir -p $dir
    file=$dir/${dt}.log
    url="http://${node}:${nmport}/ws/${version}/node/containers"
    echo $url
    curl -H "$accept" -X GET "$url" -o $file 2>/dev/null
done
