#!flask/bin/python 
from app import app
from tornado.wsgi import WSGIContainer
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop
#from pure_flask import app
http_server = HTTPServer(WSGIContainer(app))
http_server.listen(59999)
IOLoop.instance().start()

#app.run(host="0.0.0.0",port = 59999,debug = True)

