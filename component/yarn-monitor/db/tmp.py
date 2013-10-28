# class mulitDict:
#     def __init__(self,m,c,depth):
#         self.depth = depth
#         self.c = c
#         self.m = m
#         self.data = {}
#         
#     def getObject(self,*key):
#         return self.getObject()
#     
#     def getO(self):
#         module = __import__(self.m)
#         print dir(module)
#         c = getattr(module,"nmRecord")
#         print dir(c)
#         print type(c)
#         cla = getattr(c,"nmRecord")
#         print type(cla)
#         return cla()
# 
# dict = mulitDict("db.nmRecord","nmRecord",1)
# print dict.getO().getHaha()