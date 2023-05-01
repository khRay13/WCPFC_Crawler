import os, time, shutil
from selenium import webdriver
from datetime import datetime as dt
from time import sleep
# from pathlib import Path

try:
	from mail import Mail
except:
	from .mail import Mail


class Crawler():
	def __init__(self):
		self.today = dt.today()
		self.year  = str(self.today.year)
		self.mon   = str(self.today.month)
		self.day   = str(self.today.day)

		self.DLDir = "D:\\Crawler\\Download\\WCPFC"
		if not os.path.isdir(self.DLDir):
			os.mkdir(self.DLDir)

		self.TGTDir = "-".join([self.year, [self.mon if len(self.mon)>1 else "0"+self.mon][0]])
		self.TGTPath = os.path.join(self.DLDir, self.TGTDir)

		self.DRIVER = "D:\\Crawler\\Tools\\chromedriver.exe"
		self.URL    = "https://www.wcpfc.int/record-fishing-vessel-database/export"

	def startDriver(self):
		options = webdriver.ChromeOptions() # 開啟設定
		# 修改下載位置download.default_directory
		prefs = {"profile.default_content_settings.popups": 0, "download.default_directory": self.DLDir}
		options.add_experimental_option("prefs", prefs)
		options.add_experimental_option("excludeSwitches", ["enable-logging"]) # 不輸出log
		# 202303061201 打開介面模式才能在236 download成功
		# options.add_argument("--headless")  # 無介面
		options.add_argument("--disable-gpu")
		options.add_argument("--incognito") # 無痕模式

		# executable_path : driver的位置 預設會吃PATH路徑
		try:
			driver = webdriver.Chrome(executable_path = self.DRIVER, chrome_options = options)
			driver.get(self.URL)
			# 60秒等待下載 預估30秒載好 可以再拉長
			# 2021/10/25放寬到90秒
			# 2022/05/03放寬到300秒(5min)
			# 2023/03/06改回120秒(2min)
			sleep(120)

			# 關閉driver
			driver.quit()
			return [True, ""]
		except Exception as e:
			if "driver" in locals() or "driver" in globals():
				driver.quit()

			# # 初始化Mail模組
			# mail = Mail()
			# # 建立mail內容
			# m = str(self.today.strftime("%Y-%m-%d %r")) + " Crawler failed."
			# m += "\n" *2
			# m += "Message：%s\n"%(str(e)) + "\n"
			# m += "\n" *3
			# m += "This mail is automatically sent by system, do not reply!  Thanks for your cooperation!"

			# frnm = "OCR Server"; sender = "OCR@mail.idv.tw" # 寄件者名稱 / # 寄件者信箱
			# tonm = "Ray"; receivers = ["ray@mail.idv.tw"] # 收件者清單 / # 收件者信箱
			# ccnm = "Ray"; cc = ["ray@mail.idv.tw"] # 知會人員清單 / # 知會人員信箱
			# sbj = "【IMPORTANT】 WCPFC {} Crawler Update Failed".format(str(self.today.strftime("%Y-%m-%d %r"))) # 主旨
			# mail.sentMail(fr=sender, frnm=frnm, to=receivers, tonm=tonm, cc=cc, ccnm=ccnm, sbj=sbj, m=m)

			return [False, str(e)]


	def cvtName(self):
		fileList = []

		if not os.path.isdir(self.TGTPath):
			os.mkdir(self.TGTPath)

		try:
			for root, dirs, files in os.walk(self.DLDir):
				for file in files:
					path = os.path.join(root, file)
					filename, ext = os.path.basename(path).split(".")
					properties = os.stat(path)
					# 抓建立時間(timestamp)
					mdft = properties[-1]
					# t = dt.strftime(dt.utcfromtimestamp(properties[-1]), "%Y-%m-%d %H:%M:%S")
					if not fileList:
						fileList = [path, filename, ext, mdft]
					else:
						if mdft > fileList[-1]:
							# 時間大於目前讀到的檔案
							fileList = [path, filename, ext, mdft]
			else:
				new_fileName = fileList[1]+"_"+[self.day if len(self.day)>1 else "0"+self.day][0]+"."+fileList[2]
				new_filePath = os.path.join(self.TGTPath, new_fileName)
				shutil.move(fileList[0], new_filePath)
		except Exception as e:
			return [False, "", str(e)]
		else:
			return [True, new_filePath, ""]

# if __name__ == "__main__":
# 	crawler = Crawler()
# 	crawler.startDriver()
# 	newPath = crawler.cvtName()
# 	print(newPath)