import os, time
# 目前位置的絕對路徑
# baseDir = os.path.dirname(os.path.abspath(__file__))
# LogDIR_root = baseDir+"/ExecLogs"
LogDIR_root = "D:/Crawler/Log/"

try:
	os.mkdir(LogDIR_root)
except:
	pass

class servant():
	def current_time(self):
		current = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
		return current

	def saveLog(self, eventtype=None, event=0):
		with open(os.path.join(LogDIR_root, "Exec-Log-"+time.strftime("%Y-%m-%d", time.localtime())+".txt"), "a", encoding="utf-8") as rrw:
			if eventtype == "sys":
					rrw.writelines("[SYS] "+self.current_time()+" "+str(event))
					rrw.write("\n")
			elif eventtype == "conn":
					rrw.writelines("[CONN] "+self.current_time()+" "+str(event))
					rrw.write("\n")
			elif eventtype == "data":
					rrw.writelines("[DATA] "+self.current_time()+" "+str(event))
					rrw.write("\n")
			rrw.close()