function setCookie(c_name,value){
	document.cookie=c_name+ "=" +escape(value)
}
function getCookie(c_name){
	if (document.cookie.length>0){
	  c_start=document.cookie.indexOf(c_name + "=")
	  if (c_start!=-1){ 
	    c_start=c_start + c_name.length+1 
	    c_end=document.cookie.indexOf(";",c_start)
	    if (c_end==-1) c_end=document.cookie.length
	    return unescape(document.cookie.substring(c_start,c_end))
	    } 
	  }
	return ""
}
//***********
function toGSize(bytes){
	return (bytes/(1024*1024*1024)).toFixed(2); 
}
function toMSize(bytes){
	return (bytes/(1024*1024)).toFixed(2); 
}
//***********
//状态转换
function toState(state){
	var map = {"FINISHED":"成功","FAILED":"失败","KILLED":"中止"}
	return map[state];
}
function toFinalStatus(finalStatus){
	var map = {"SUCCEEDED":"成功","FAILED":"失败","KILLED":"中止","UNDEFINED":"未定义"}
	return map[finalStatus];
}
//***********
//object length
function getlength(obj){
	var temp = 0;
	for(var id in obj){
		temp++;
	}
	return temp;
}
//***********
function incDomText(selecter,inc){
	var now = parseInt($(selecter).text())
	now += inc;
	$(selecter).text(now)
}
//***********
function datetime_to_unix(datetime){
	if( datetime.length < 4  ) return null;
    var tmp_datetime = datetime.replace(/:/g,'-');
    tmp_datetime = tmp_datetime.replace(/ /g,'-');
    var arr = tmp_datetime.split("-");
    if( arr.length  < 6 ){
    	arr[5] = 0
    }
    var now = new Date(Date.UTC(arr[0],arr[1]-1,arr[2],arr[3]-8,arr[4],arr[5]));
    return parseInt(now.getTime()/1000);
}
function unix_to_datetime(unix) {
    var now = new Date(parseInt(unix));
    return now.getFullYear()+"-"+padZero(now.getMonth()+1,2)+"-"+padZero(now.getDate(),2)+" "
    	+padZero(now.getHours(),2)+":"+padZero(now.getMinutes(),2)+":"+padZero(now.getSeconds());
}
function unix_to_datetimeNoSecond(unix) {
    var now = new Date(parseInt(unix));
    return now.getFullYear()+"-"+padZero(now.getMonth()+1,2)+"-"+padZero(now.getDate(),2)+" "
    	+padZero(now.getHours(),2)+":"+padZero(now.getMinutes(),2);
}
function unix_to_datetimeInHighchart(unix) {
    var now = new Date(parseInt(unix));
    return now.getFullYear()+"-"+padZero(now.getMonth()+1,2)+"-"+padZero(now.getDate(),2)+"<br>"
    	+padZero(now.getHours(),2)+":"+padZero(now.getMinutes(),2);
}
function padZero(inStr, length) {
	var str = String(inStr)
    var strLen = str.length;
    return length > strLen ? new Array(length - strLen + 1).join("0") + str : str;
}
function get_now_time(){
	return new Date().valueOf();
}
function get_unix_time(){
	return Math.floor(new Date().valueOf()/1000);
}
//***********
function getNodeFromAddress(address){
	length = address.indexOf(":");
	return address.substring(0,length);
}
//***********
function contain(str,want){
	l = str.indexOf(want)
	if(l == -1) return false;
	else return true;
}
//***********
function getTrTd(list){
	var re = "";
	for (var id in list){
		re += "<td id="+id+">"+list[id]+"</td>"
	}
	return "<tr>"+re+"</tr>"
}
//恶心的代码根据Map和Reduce开头判断横跨的列
function getTrTh(list){
	var re = "";
	for (var key in list){
		var temp = list[key]
		if(contain(temp,"Map")){
			re += "<th colspan=6 >"+list[key]+"</th>"
		}
		else if(contain(temp,"Reduce")){
			re += "<th colspan=6 >"+list[key]+"</th>"
		}
		else{
			re += "<th >"+list[key]+"</th>"
		}
	}
	return "<tr>"+re+"</tr>"
}
function getTable(id,title,contentList){
	var body = "";
	for (var key in contentList){
		body += getTrTd(contentList[key])
	}
	return '<table class="table table-bordered table-striped table-hover" id="'+id+'">'+
				'<thead>'+getTrTh(title)+'</thead>'+
				'<tbody>'+body+'</tbody></table>';
}
function baseFormat(formatY) {
    var s = '<b>'+ this.x +'</b>';
    
    $.each(this.points, function(i, point) {
    	var temp = point.y;
        s += '<br/>'+ point.series.name +': '+ temp;
    });   
    return s;
}
function getValueFormatter(field){
	var formatY = null;
	switch(field){
		case "hdfsWrite":
		case "hdfsRead":
		case "fileRead":
		case "fileWrite": formatY = (function(y){ return (y/(1024*1024*1024)).toFixed(3) +" GB"; });break;
		case "mapTime":
		case "reduceTime":formatY = (function(y){return (y/(1000)).toFixed(3)+" S"; });break;
		default:formatY = (function(y){return y +" 个"; });break;
	}
	return formatY;
}
function buildLineCharts(htmlId,title,xAxis,formatY,series){
	var tickInterval = Math.floor( xAxis.length/4 )+1
//	console.log(xAxis.length)
//	console.log(tickInterval)
	var temp = {
		    chart: {
		        type: 'line'
		    },
		    title: {
		        text: title,
		        x: -20 //center
		    },
		    subtitle: {
		        text: ' ',
		        x: -20
		    },
		    xAxis: {
		        categories: xAxis,
		        labels:{ 
		            step:1,
		            rotation: -25,	
//		            align: 'right',	
		            style: { font: 'normal 13px Verdana, sans-serif'}
		        },
		    	tickInterval: tickInterval
		    },
		    yAxis: {
		        title: {
		            text: ' '
		        },
		        min: 0 ,
		        plotLines: [{
		            value: 0,
		            width: 1,
		            color: '#808080'
		        }]
		    },
		    tooltip: {
		    	shared: true,
		        formatter:function() {
	                var s = '<b>'+ this.x +'</b>';
	                
	                $.each(this.points, function(i, point) {
	                	var temp = formatY(point.y)
	                    s += '<br/>'+ point.series.name +': ' + temp;
	                });
	                
	                return s;
	            },
		    },
		    plotOptions: {
		        line: {
		            marker: {
		                radius: 1
		            }
		        }
		    },
		    credits: {
		        'text': 'myhadoop',
		        'href': 'http://github.com/zouhc/MyHadoop'
		   },

		    'series': series
		}
	$("#"+htmlId).highcharts(temp);
}
