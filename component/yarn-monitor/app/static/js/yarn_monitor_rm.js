function drawRmHighChart(htmlid,field,dataPool,index,beginTime,endTime,split){
	var begin = Math.floor(beginTime/split)*split;
	var end = Math.floor(endTime/split)*split;
	var xAxis = [];
	for(var i = begin;i<=end;i+=split){
		xAxis.push(unix_to_datetimeInHighchart(i*1000))
	}
	var series = [];

	var temp = new Array();
	for(var i = begin;i<=end;i+=split){
		if( (i in dataPool) ){
			if( dataPool[i][index] != null){
				temp.push(dataPool[i][index])
				continue;
			}
		}
		temp.push(0)
	}
	series.push({name:field,data:temp})
	
	buildLineCharts(htmlid,field,xAxis,getValueFormatter(field),series)	
}
function showRmData(data){
	//分隔数据到各个指标
	var fields = getRmFieldParams();
	var happenTimeMin = getRmHappenTimeMinParams();
	var happenTimeMax = getRmHappenTimeMaxParams();
	var happenTimeSplit = getRmHappenTimeSplitParams();
	//转换data的result的记录形式从[time,xx,xx...]转换为[time][xx,xx...]
	var dataPool = {}
	console.log(data);
	for(var key in data['result']){
		var temp = data['result'][key];
		var time = temp[0]
		for(var i=1;i<temp.length;i++){
			if( !(time in dataPool) ){
				dataPool[time] = {}
			}
			dataPool[time][i-1]=temp[i];
		}
	}
	//添加各个区域的DIV
	$("#rm-draw-div").empty()
	for(var i=0,columnIndex=0,rowIndex=0;i<fields.length;i++){
		if(columnIndex==0){
			$("#rm-draw-div").append("<div class='row-fluid' id='rm-draw-div-row-"+rowIndex+"'></div>");
		}
		$("#rm-draw-div-row-"+rowIndex).append("<div class='span4'><div id='rm-draw-div-table-"+i+"' class='draw-div-table'></div></div>")
		columnIndex++;
		if(columnIndex==3) {
			rowIndex++;
			columnIndex=0;
		}
	}
	//绘图
	for(var i=0;i<fields.length;i++){
		var field = fields[i]
		var htmlid = "rm-draw-div-table-"+i;
		drawRmHighChart(htmlid,field,dataPool,i,happenTimeMin,happenTimeMax,happenTimeSplit)
	}	  
}
function loadRmData(){
	var fields = getRmFieldParams();
	
	var happenTimeMin = getRmHappenTimeMinParams();
	var happenTimeMax = getRmHappenTimeMaxParams();
	var happenTimeSplit = getRmHappenTimeSplitParams();
	
	var appQuery;
	appQuery = new XMLHttpRequest();
	appQuery.onreadystatechange=function(){
		if (appQuery.readyState==4 && appQuery.status==200){
			showRmData(JSON.parse(appQuery.responseText));
    	}
  	}
	var url = "db/rmQuery?fields="+fields+"&happenTimeSplit="+happenTimeSplit
			+"&happenTimeMax="+happenTimeMax+"&happenTimeMin="+happenTimeMin
	appQuery.open("GET",url,true);
	appQuery.send();
}
function changeRmHappenTimeParams(){
	var value = $("#rm-params-happenTime-select option:selected").attr("value")
	if( value == -2 ){
		$("#rm-params-happenTime-divs")[0].style.display = "inline-block";
		$("#rm-params-happenTime-min")[0].value = unix_to_datetimeNoSecond(get_now_time()-(24*3600*1000));
		$("#rm-params-happenTime-max")[0].value = unix_to_datetimeNoSecond(get_now_time());
	}
	else{
		$("#rm-params-happenTime-divs")[0].style.display = "none";
		$("#rm-params-happenTime-min")[0].value = unix_to_datetimeNoSecond(get_now_time()-(value*60000));
		$("#rm-params-happenTime-max")[0].value = unix_to_datetimeNoSecond(get_now_time());
	}
}
function getRmFieldParams(){
	var pick = $("#rm-params-field-select option:selected");
	var temp = []
	for(var i=0;i<pick.length;i++){
	    if(  pick[i].value == "app" ){
			temp.push("appNum")
			temp.push("finishedApp")
			temp.push("failedApp")
			temp.push("killedApp")
			temp.push("notSuccApp")
		}
	    else if(  pick[i].value == "map" ){
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
function getRmHappenTimeSplitParams(){
	var pick = $("#rm-params-happenTime-split-select option:selected");
	return pick[0].value*60;
}
function getRmHappenTimeMinParams(){
	var temp = datetime_to_unix($("#rm-params-happenTime-min")[0].value);
	if(temp == null){
		temp = get_unix_time()-24*3600;
	}
	//temp = temp - 24*3600*20;
	return temp;
}
function getRmHappenTimeMaxParams(){
	var temp = datetime_to_unix($("#rm-params-happenTime-max")[0].value);
	if(temp == null){
		temp = get_unix_time();
	}
	return temp;
}
function rmQuery(){
	loadRmData()
}
function rmInit(){
	$("#rm-params-field-select").chosen({width:"600px"});
	$(".form_datetime").datetimepicker({format: 'yyyy-mm-dd hh:ii'});
	rmQuery()
}