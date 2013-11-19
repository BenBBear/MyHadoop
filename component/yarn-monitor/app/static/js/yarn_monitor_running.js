//获取要展示的队列的html内容
function loadRunningState(){
	var appQuery;
	appQuery = new XMLHttpRequest();
	appQuery.onreadystatechange=function(){
		if (appQuery.readyState==4 && appQuery.status==200){
			var response = JSON.parse(appQuery.responseText);
			var result = response['clusterMetrics']
			console.log(result)
			var temp ="正在运行"+result['appsRunning']+"个应用，分配了"
			+result['containersAllocated']+"个container，使用了"+result['allocatedMB']+"MB内存,"
			+"剩余"+(result['availableMB']-result['allocatedMB'])+"MB内存。存活"+result['activeNodes']+"个NodeManager。";
			$("#running-status").text(temp);
//			console.log(temp)
    	}
  	}
	var url = "db/appRunningState";
	appQuery.open("GET",url,true);
	appQuery.send();
}
function formatElapsedTime(elapsedTime){
	if( elapsedTime=="x" ) return elapsedTime;
	var sec = Math.floor(elapsedTime/1000);
	var min = Math.floor(sec/60);
	sec = sec - min*60;
	return min+"m"+sec+"s";
}
function formatTableTd(key,value){
	if( key == "startedTime"){
		return unix_to_datetime(value);
	}
	else if( key == "progress"){
		return value.toFixed(1)+"%";
	}
	else if( key == "elapsedTime"){
		return formatElapsedTime(value)
	}
	else if(key =="amHostHttpAddress"){
		return getNodeFromAddress(value)
	}
	else if(key =="id"){
		last = value.lastIndexOf("_")
		return "<a href=http://"+rmhost+":"+rmport+"/proxy/"+value+">"+value.substring(last+1)+"</a>"
	}
	return value
}
function getTitleInfoHtml(queueName,sum){
	return ':一共运行了<em id="title-'+queueName+'-sum" >'+sum+'</em>个应用.'+
			'采集了<em id="title-'+queueName+'-coll" >0</em>个应用的数据，'+
			'采集的应用正在运行<em id="title-'+queueName+'-mapsRunning">0</em>个Map'+
			'和<em id="title-'+queueName+'-reducesRunning" >0</em>个Reduce,'+
			'还需要运行<em id="title-'+queueName+'-mapsPending">0</em>个Map'+
			'和<em id="title-'+queueName+'-reducesPending" >0</em>个Reduce'
}
function getQueuePanelHtml(queueName,queue){
	var displayTitleList = new Array("应用id","用户","名称","AM机器","提交至今时间","进度","Am运行时间","Map(总数,待运行,正运行,失败,杀死,成功)","Reduce(总数,待运行,正运行,失败,杀死,成功)")
	var titleList = new Array("id","user","name","amHostHttpAddress","elapsedTime","progress")
	var contentList = new Array();
	for(var id in queue){
		var app = queue[id]
		var tds = new Array();
		for(var key in titleList){
			var title = titleList[key];
			tds[id+"-"+title]=formatTableTd(title,app[title])
		}
		//添加异步回调的td
		tds[id+"-amTime"] = "-"
		var idList = ["mapsTotal","mapsPending","mapsRunning","failedMapAttempts","killedMapAttempts","successfulMapAttempts",
		              "reducesTotal","reducesPending","reducesRunning","failedReduceAttempts","killedReduceAttempts","successfulReduceAttempts"];
		for(var key in idList){
			tds[id+"-"+idList[key]]="--"
		}
		contentList.push(tds)
	}
	var tableHtml = getTable("queueName",displayTitleList,contentList);
	return tableHtml;
}
function addQueuePanel(accordionId,collapseTitle,titleInfo,accordionBody,collapseId){
	var panelTitle ='<div class="panel-heading"><h4 class="panel-title">'+
	        '<a data-toggle="collapse" data-toggle="collapse" data-parent="#'+accordionId+'" href="#'+collapseId+'">'+
	        collapseTitle+'</a>'+titleInfo+'</h4></div>';
	var open = " in "
	var panelBody = '<div id="'+collapseId+'" class="panel-collapse collapse '+open+'"><div class="panel-body">'+
			accordionBody+'</div></div>'
	$("#running-accordion").append('<div class="panel panel-default">'+panelTitle+panelBody+'</div>');
}

function showRunningApp(runningApp){
	rmhost = runningApp['rmhost']
	rmport = runningApp['rmport']
	$("#running-accordion").empty();
	var index = 1;
	var appidList = [];
	for(var queueName in runningApp["queues"]){
		var queue = runningApp["queues"][queueName];
		console.log(queue)
		addQueuePanel("running-accordion","队列:["+queueName+"]",getTitleInfoHtml(queueName,getlength(queue)),
					getQueuePanelHtml(queueName,queue),"panel"+index);
		index++;
	}
}
function loadRunningAppInfo(appid,queueName){
	var appQuery;
	appQuery = new XMLHttpRequest();
	appQuery.onreadystatechange=function(){
		if (appQuery.readyState==4 && appQuery.status==200){
			jobinfo = JSON.parse(appQuery.responseText)
			$("#"+appid+"-amTime").text(formatElapsedTime(jobinfo['amTime']))
			var keyList = ["mapsTotal","mapsPending","mapsRunning","failedMapAttempts","killedMapAttempts","successfulMapAttempts",
			               "reducesTotal","reducesPending","reducesRunning","failedReduceAttempts","killedReduceAttempts","successfulReduceAttempts"];
			//回调更新表格中的数据
			for(var k in keyList){
				var key = keyList[k]
				$("#"+appid+"-"+key).text(jobinfo[key])
			}
			//回调更新队列的汇总信息
			//更新 title-'+queueName+'-coll"  title-'+queueName+'-map" title-'+queueName+'-reduce"
			incDomText("#title-"+queueName+"-coll",1)
			incDomText("#title-"+queueName+"-mapsRunning",jobinfo['mapsRunning'])
			incDomText("#title-"+queueName+"-reducesRunning",jobinfo['reducesRunning'])
			incDomText("#title-"+queueName+"-mapsPending",jobinfo['mapsPending'])
			incDomText("#title-"+queueName+"-reducesPending",jobinfo['reducesPending'])
		}
  	}
	var url = "/db/appProxy?appid="+appid;
	appQuery.open("GET",url,true);
	appQuery.send();
}
function loadRunningApp(){
	var appQuery;
	appQuery = new XMLHttpRequest();
	appQuery.onreadystatechange=function(){
		if (appQuery.readyState==4 && appQuery.status==200){
			var runningApp = JSON.parse(appQuery.responseText)
			//生成每个队列的的基本信息
			showRunningApp( runningApp );
			console.log(runningApp);
			//生成采集每个app的回调
			for(var queueName in runningApp["queues"]){
				var queue = runningApp["queues"][queueName];
				for(var appid in queue){
					loadRunningAppInfo(appid,queueName)
				}
			}
    	}
  	}
	var url = "db/appRunning";
	appQuery.open("GET",url,true);
	appQuery.send();
}
function showWaittingApp(waittingApp){
	rmhost = waittingApp['rmhost']
	rmport = waittingApp['rmport']
	$("#waitting-table-body").empty();
	if( waittingApp['waitting'] != null){
		for(var key in waittingApp['waitting']['app']){
			app = waittingApp['waitting']['app'][key]
			td = "<td>"+formatTableTd("id",app["id"])+"</td>";
			td = td+"<td>"+formatTableTd("user",app["user"])+"</td>";
			td = td+"<td>"+formatTableTd("name",app["name"])+"</td>";
			td = td+"<td>"+formatTableTd("queue",app["queue"])+"</td>";
			td = td+"<td>"+formatTableTd("elapsedTime",app["elapsedTime"])+"</td>";
			tr="<tr>"+td+"</tr>";
			$("#waitting-table-body").append(tr);
		}
	}
	
}
function loadWaittingApp(){
	var appQuery;
	appQuery = new XMLHttpRequest();
	appQuery.onreadystatechange=function(){
		if (appQuery.readyState==4 && appQuery.status==200){
			showWaittingApp(JSON.parse(appQuery.responseText));
    	}
  	}
	var url = "db/appWaitting";
	appQuery.open("GET",url,true);
	appQuery.send();
}
function runningQuery(){
	loadRunningState()
	loadRunningApp()
	loadWaittingApp()
}
function runningInit(){
	
}