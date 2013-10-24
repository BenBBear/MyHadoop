function drawNmHighChart(htmlid,field,dataPool,index,hosts,beginTime,endTime,split){
	var begin = Math.floor(beginTime/split)*split;
	var end = Math.floor(endTime/split)*split;
	var xAxis = [];
	for(var i = begin;i<=end;i+=split){
		xAxis.push(unix_to_datetimeInHighchart(i*1000))
	}
	var series = [];
	for(var key in hosts){
		var temp = new Array();
		var host = hosts[key]
		for(var i = begin;i<=end;i+=split){
			if( (i in dataPool) && (host in dataPool[i]) ){
				if( dataPool[i][host][index] != null){
					temp.push(dataPool[i][host][index])
					continue;
				}
			}
			temp.push(0)
		}
		series.push({name:host,data:temp})
	}
//	buildLineCharts(htmlid,field,xAxis," ",series)
	buildLineCharts(htmlid,field,xAxis,getValueFormatter(field),series)	
}
function showNmData(data){
	//分隔数据到各个指标
	var fields = getNmFieldParams();
	var hosts = getNmHostParams();
	var happenTimeMin = getNmHappenTimeMinParams();
	var happenTimeMax = getNmHappenTimeMaxParams();
	var happenTimeSplit = getNmHappenTimeSplitParams();
	//转换data的result的记录形式从[time,host,xx,xx...]转换为[time][host][xx,xx...]
	var dataPool = {}
	for(var key in data['result']){
		var temp = data['result'][key];
		var time = temp[0]
		var host = temp[1]
		for(var i=2;i<temp.length;i++){
			if( !(time in dataPool) ){
				dataPool[time] = {}
			}
//			console
			if( !(host in dataPool[time]) ){
				dataPool[time][host] = new Array()
			}
			dataPool[time][host][i-2]=temp[i];
		}
	}
	//添加各个区域的DIV
	$("#nm-draw-div").empty()
	for(var i=0,columnIndex=0,rowIndex=0;i<fields.length;i++){
		if(columnIndex==0){
			$("#nm-draw-div").append("<div class='row-fluid' id='nm-draw-div-row-"+rowIndex+"'></div>");
		}
		$("#nm-draw-div-row-"+rowIndex).append("<div class='span4'><div id='nm-draw-div-table-"+i+"' class='draw-div-table'></div></div>")
		columnIndex++;
		if(columnIndex==3) {
			rowIndex++;
			columnIndex=0;
		}
	}
	//绘图
	for(var i=0;i<fields.length;i++){
		var field = fields[i]
		var htmlid = "nm-draw-div-table-"+i;
		drawNmHighChart(htmlid,field,dataPool,i,hosts,happenTimeMin,happenTimeMax,happenTimeSplit)
	}	               
	
}
function loadNmData(){
	var hosts = getNmHostParams();
	var fields = getNmFieldParams();
	
	var happenTimeMin = getNmHappenTimeMinParams();
	var happenTimeMax = getNmHappenTimeMaxParams();
	var happenTimeSplit = getNmHappenTimeSplitParams();
	
	var appQuery;
	appQuery = new XMLHttpRequest();
	appQuery.onreadystatechange=function(){
		if (appQuery.readyState==4 && appQuery.status==200){
			showNmData(JSON.parse(appQuery.responseText));
    	}
  	}
	var url = "db/nmQuery?hosts="+hosts+"&fields="+fields+"&happenTimeSplit="+happenTimeSplit
			+"&happenTimeMax="+happenTimeMax+"&happenTimeMin="+happenTimeMin
	appQuery.open("GET",url,true);
	appQuery.send();
}
function loadNmHost(){
	$("#nm-params-host-select").empty();
	var params = getParams();
	var appQuery;
	appQuery = new XMLHttpRequest();
	appQuery.onreadystatechange=function(){
		if (appQuery.readyState==4 && appQuery.status==200){
			
			var hostJson = JSON.parse(appQuery.responseText);
			var autoSelect = 2;
			for(var key in hostJson){
				var host = hostJson[key];
				if( autoSelect > 0 ){
					$("#nm-params-host-select").append("<option selected='selected' value='"+host+"'>"+host+"</option>");
				}
				else{
					$("#nm-params-host-select").append("<option value='"+host+"'>"+host+"</option>");
				}
				autoSelect = autoSelect-1;
			}
			$("#nm-params-host-select").chosen({width:"600px"});
    	}
  	}
	var url = "db/hostlist";
	appQuery.open("GET",url,false);
	appQuery.send();
}
function changeNmHappenTimeParams(){
	var value = $("#nm-params-happenTime-select option:selected").attr("value")
	if( value == -2 ){
		$("#nm-params-happenTime-divs")[0].style.display = "inline-block";
		$("#nm-params-happenTime-min")[0].value = unix_to_datetimeNoSecond(get_now_time()-(24*3600*1000));
		$("#nm-params-happenTime-max")[0].value = unix_to_datetimeNoSecond(get_now_time());
	}
	else{
		$("#nm-params-happenTime-divs")[0].style.display = "none";
		$("#nm-params-happenTime-min")[0].value = unix_to_datetimeNoSecond(get_now_time()-(value*60000));
		$("#nm-params-happenTime-max")[0].value = unix_to_datetimeNoSecond(get_now_time());
	}
}
function getNmHostParams(){
	var pick = $("#nm-params-host-select option:selected");
	var temp = new Array(pick.length)
	for(var i=0;i<pick.length;i++){
		temp[i] = pick[i].value;
	}
	return temp;
}
function getNmFieldParams(){
	var pick = $("#nm-params-field-select option:selected");
	var temp = []
	for(var i=0;i<pick.length;i++){
		if(  pick[i].value == "map" ){
			temp.push("mapNum")
			temp.push("mapTime")
			temp.push("failMap")
		}
		else if(  pick[i].value == "reduce" ){
			temp.push("reduceNum")
			temp.push("reduceTime")
			temp.push("failReduce")
		}
		else if(  pick[i].value == "file" ){
			temp.push("fileRead")
			temp.push("fileWrite")
		}    
		else if(  pick[i].value == "hdfs" ){
			temp.push("hdfsRead")
			temp.push("hdfsWrite")
		}    
		else{
			temp.push(pick[i].value)
		}
	}
	return temp;
}
function getNmHappenTimeSplitParams(){
	var pick = $("#nm-params-happenTime-split-select option:selected");
	return pick[0].value*60;
}
function getNmHappenTimeMinParams(){
	var temp = datetime_to_unix($("#nm-params-happenTime-min")[0].value);
	if(temp == null){
		temp = get_unix_time()-24*3600;
	}
	//temp = temp - 24*3600*20;
	return temp;
}
function getNmHappenTimeMaxParams(){
	var temp = datetime_to_unix($("#nm-params-happenTime-max")[0].value);
	if(temp == null){
		temp = get_unix_time();
	}
	return temp;
}
function nmQuery(){
	loadNmData()
}
function nmInit(){
	loadNmHost()
	$("#nm-params-field-select").chosen({width:"600px"});
	$(".form_datetime").datetimepicker({format: 'yyyy-mm-dd hh:ii'});
//	console.log(getNmHostParams())
	nmQuery()
}