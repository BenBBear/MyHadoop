import json
from app import app 
from app import data 
from flask import render_template
from flask import request
from flask import Markup

import time

@app.route('/') 
@app.route('/index') 
@app.route('/lookapp') 
def index():
    now = time.localtime();
    today = '%04d%02d%02d' % (now[0], now[1] ,now[2]) 
    date = request.args.get("date",today)
    start = request.args.get("start",0,int)
    end = request.args.get("end",start,int)
    type = request.args.get("type","app",str)
    top = request.args.get("top", 10 ,int)

    #if end >= start +2 : 
    #    end = start +1
    if 0 <= start and start <= end and end <= 24 :
        try:
            (xaxis,series,appseries) = data.node_container_data(type,date,start,end,top)
        except Exception,data:
            return "ERROR with exception while getting data "
        return gethtml(xaxis,series,appseries,type,today,start,end,top)
    else :
        return "ERROR with start=" + str(start) +" end="+ str(end)

def gethtml(xaxis,series,appseries,type,date,start,end,top):
    if start == 0 :
        pre_start = 0;
    else :
        pre_start = start - 1;

    if start == 23 :
        next_start = 23;
    else :
        next_start = start + 1;

    return render_template("look.html",
            debug = Markup("debug info here"),
            date = date,
            start = start,
            pre_start = pre_start,
            next_start = next_start,
            end = end,
            top = top,
            type = type,
            xaxis = Markup(json.dumps(xaxis)),
            series = Markup(json.dumps(series)),
            series2 = Markup(json.dumps(appseries)),
            )

