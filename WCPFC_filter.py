import os, time, re
import pandas as pd
from time import sleep
from datetime import datetime as dt

import inspect as isp
lineno = lambda: isp.currentframe().f_back.f_lineno

try:
	from mail import Mail
	from Tools.SqlServerConn import Conn
	from Tools.servant import servant

	from crawler import Crawler
	from JDEConn import JDE

except:
	from .mail import Mail
	from .Tools.SqlServerConn import Conn
	from .Tools.servant import servant

	from .crawler import Crawler
	from .JDEConn import JDE

mail = Mail()
Conn = Conn()
Serv = servant()
JDE = JDE()
SpeKey = ["YES", "NAN", "NONE", "UNKNOWN", "NOT SPECIFIED", "NOT APPLICABLE"]
regularVSLName = lambda vn: str(vn).replace(" ", "").upper().strip()
testMode = False

class Utils:
	def getTime(self, t = None):
		if not t:
			t = dt.today()
		day = str(t.strftime("%Y-%m-%d"))
		nowtime = t.strftime("%Y-%m-%d %H:%M:%S")
		return [day, nowtime]

	def cvtTimeFmt(self, t):
		t = re.sub(r"[ -]", "", t).strip()
		if t != "":
			try:
				t = dt.strptime(t, "%d%b%Y")
			except:
				t = dt.strptime(t, "%d%b%y")
			t = t.strftime("%d-%b-%y")
		return t

	def updVesselData(self, CallSign=None, IMO=None, VSLName=None, cols=None, vals=None):
		# CallSign: str, IMO: str, VSLName: str, cols: list, vals: list
		if testMode:
			pass
		else:
			Conn.Open()
			comm = Conn.Update(
				table = "VesselData",
				columns = cols,
				values = vals,
				conditions = f"IRCS='{CallSign}' AND IMO='{IMO}' AND VSLName1='{VSLName}' AND ORG='WCPFC'"
			)
			Conn.Close()
			sleep(0.1)

	def istVesselData(self, vals):
		cols = tuple([
			"VID", "WIN", "IMO", "RegsNum", "IRCS", "Flag",
			"ORG", "VSLName1",
			"OwnrName", "OwnrAddr",
			"VSLType", "BuiltYear",
			"VSLCpcy", "VSLCpcyUnit",
			"AuthFrom", "AuthTo",
			"_State", "Flag_sin", "Origin",
		])

		if testMode:
			pass
		else:
			Conn.Open()
			comm = Conn.Insert(
				table = "VesselData",
				columns = cols,
				values = vals
			)
			Conn.Close()
			sleep(0.1)

	def cprVesselData(self, updData, xlsData, dbsData):
		cpr = all([True for k, d in enumerate(xlsData[:-3]) if d == dbsData[k]])
		if not cpr or str(dbsData[-1]) == "0":
			# 若出現不同的資料，從Excel更新到DB中
			vals = [
				xlsData[0], xlsData[1], xlsData[2], xlsData[3],
				xlsData[4], VSLs[k], xlsData[5], xlsData[6],
				xlsData[7], xlsData[8], xlsData[9], xlsData[10],
				xlsData[11], xlsData[12], xlsData[13], "1", xlsData[15],
			]
			updData.append(vals)
			self.updVesselData(
				CallSign = dbsData[4],
				IMO = dbsData[2],
				VSLName = dbsData[6],
				cols = tuple([
					"VID", "WIN", "IMO", "RegsNum", "IRCS",
					"VSLName1", "Flag",
					"OwnrName", "OwnrAddr",
					"VSLType", "BuiltYear",
					"VSLCpcy", "VSLCpcyUnit",
					"AuthFrom", "AuthTo",
					"_State", "Origin"
				]),
				vals = vals)
			sleep(0.1)
		return updData

	def failedMail(self, m):
		frnm = "OCR Server"; sender = "OCR@mail.idv.tw" # 寄件者名稱 / # 寄件者信箱
		if testMode:
			tonm = "Ray"; receivers = ["ray@mail.idv.tw"] # 收件者清單 / # 收件者信箱(測試)
			sbj = "【Test】 WCPFC {} Execute failure".format(str(self.getTime()[1])) # 主旨(測試)
		else:
			tonm = "Ray"; receivers = ["ray@mail.idv.tw"] # 收件者清單 / # 收件者信箱
			sbj = "【Notice】 WCPFC {} Execute failure".format(str(self.getTime()[1])) # 主旨
		mail.sentMail(fr=sender, frnm=frnm, to=receivers, tonm=tonm, cc="", sbj=sbj, m=m)

class Cleaner(Utils):
	@staticmethod
	def FlagCvt(f):
		f = f.replace(" ", "")
		return {
			"Australia":"AT",
			"ChineseTaipei":"TW", "CookIslands":"CK", "Canada":"CA", "Croatia":"HR", "CostaRica":"CR", "Curacao":"CW", "China":"CN",
			"ElSalvador":"SV", "Ecuador":"EC",
			"Fiji":"FJ", "FederatedStatesofMicronesia":"FM", "France":"FR", "Finland":"FI", "FrenchPolynesia":"PF",
			"Germany":"DE",
			"Indonesia":"ID", "Italy":"IT",
			"Japan":"JP", "Jordan":"JO",
			"Korea(Republicof)":"KR", "Kiribati":"KI",
			"Lithuania":"LT", "Liberia":"LR",
			"Myanmar":"MM", "Mexico":"MX", "Marshall Islands":"MH",
			"NewZealand":"NZ", "Nicaragua":"NI", "Nauru":"NR", "Netherlands":"NL", "Norway":"NO", "New Caledonia":"NC",
			"Philippines":"PH", "Panama":"PA", "PapuaNewGuinea":"PG", "Portugal":"PT", "Pakistan":"PK", "Poland":"PL",
			"Spain":"ES", "SolomonIslands":"SB", "Singapore":"SG",
			"Thailand":"TH", "Tuvalu":"TV", "Tonga":"TO",
			"UnitedStatesofAmerica":"US", "Uruguay":"UY",
			"Vanuatu":"VU",
		}.get(f, f)

	def Data2DB(self, df):
		datas = []
		VSLs = []

		VID = [k for k in df["VID"]]
		WIN = [k for k in df["WIN"]]

		# 處理IMO
		IMO = []
		for n, k in enumerate(df["IMO-LR"]):
			if str(k).upper().strip() in SpeKey:
				IMO.append("")
			else:
				IMO.append(str(int(k)))

		RegsNum = []
		for n, k in enumerate(df["Registration Number"]):
			if str(k).upper().strip() in SpeKey:
				RegsNum.append("")
			else:
				RegsNum.append(str(k))

		# 處理CallSign
		IRCS = []
		for n, k in enumerate(df["IRCS"]):
			if str(k).upper().strip() in SpeKey:
				IRCS.append("")
			else:
				IRCS.append(str(k))

		Flag = [k for k in df["Flag"]]
		Flag_sin = [self.FlagCvt(k) for k in df["Flag"]]

		VesselName = [k for k in df["Vessel Name"]]
		OwnrName = [k.replace("'", "`") for k in df["Owner Name"]]

		# OwnrAddr
		OwnrAddr = []
		for n, k in enumerate(df["Owner Address"]):
			if str(k).upper().strip() in SpeKey:
				OwnrAddr.append("")
			else:
				OwnrAddr.append(k.replace("\"", "``").replace("'", "`"))
		VesselType = [k for k in df["Vessel Type"]]

		# BuiltYear
		BuiltYear = []
		for n, k in enumerate(df["Built in Year"]):
			if str(k).upper().strip() in SpeKey:
				BuiltYear.append("")
			else:
				BuiltYear.append(str(int(k)))

		# FishCpcy
		FishCpcy =  []
		for n, k in enumerate(df["FishHold Capacity"]):
			if str(k).upper().strip() in SpeKey:
				FishCpcy.append("")
			else:
				FishCpcy.append(str(int(k)))

		# FishCpcyUnit
		FishCpcyUnit = []
		for n, k in enumerate(df["FishHold Cap Units"]):
			if str(k).upper().strip() in SpeKey:
				FishCpcyUnit.append("")
			else:
				FishCpcyUnit.append(str(k))

		#AuthTime
		AuthFrom = []
		AuthTo = []
		for n in range(len(df["Auth Period From"])):
			auFm = df["Auth Period From"][n]
			auTo = df["Auth Period To"][n]
			if str(auFm).upper().strip() in SpeKey:
				AuthFrom.append("")
			else:
				AuthFrom.append(auFm)

			if str(auTo).upper().strip() in SpeKey:
				AuthTo.append("")
			else:
				AuthTo.append(auTo)

		for n in range(len(VID)):
			datas.append([
				str(VID[n]), WIN[n], IMO[n], RegsNum[n], IRCS[n], Flag[n],
				OwnrName[n], OwnrAddr[n],
				VesselType[n], BuiltYear[n],
				FishCpcy[n], FishCpcyUnit[n],
				self.cvtTimeFmt(AuthFrom[n]), self.cvtTimeFmt(AuthTo[n]), "1", # 1 for isCrawler
				# 20210910新增
				Flag_sin[n]
			])

			VSLs.append(VesselName[n].replace("'", "`"))

		return VSLs, datas

	def Data2JDE(self, df):
		# VSLName # TTDL01B
		# CallSign # TTDL01C
		# IMO # TTDL01D
		# ORG # TTDL01H

		# 1. VSLName
		VesselName = [k for k in df["Vessel Name"]]
		# 2. CallSign
		IRCS = []
		for n, k in enumerate(df["IRCS"]):
			if str(k).upper().strip() in SpeKey:
				IRCS.append("")
			else:
				IRCS.append(str(k))
		# 3. IMO
		IMO = []
		for n, k in enumerate(df["IMO-LR"]):
			if str(k).upper().strip() in SpeKey:
				IMO.append("")
			else:
				IMO.append(str(int(k)))
		# 4. AuthDate
		AuthDate = []
		for k, v in enumerate(zip(df["Auth Period From"], df["Auth Period To"])):
			auFm = "" if str(v[0]).upper().strip() in SpeKey else v[0]
			auTo = "" if str(v[1]).upper().strip() in SpeKey else v[1]
			AuthDate.append(f"{self.cvtTimeFmt(auFm)} / {self.cvtTimeFmt(auTo)}")

		return [VesselName, IRCS, IMO, AuthDate]



# Initial Object
Utils = Utils()
Cleaner = Cleaner()
Crawler = Crawler()
ts = time.time()
Serv.saveLog("sys", "Crawler Start.")
if testMode:
	CW_rst = [True, ""]
else:
	CW_rst = Crawler.startDriver()


if CW_rst[0]:
	Serv.saveLog("sys", "Crawler Down. Cost -> %ss"%(str(int(time.time() - ts))))

	# 偵測Chrome下載失敗
	stat, newPath, erMsg = Crawler.cvtName(); Serv.saveLog("data", "File at: %s"%(str(newPath)))
	if stat:
		df = pd.read_csv(newPath)
	else:

		# 移除下載失敗的檔案
		try:
			os.remove("D:\\Crawler\\Download\\WCPFC\\RFV_database_export.csv.crdownload")
			Serv.saveLog("sys", "Delete RFV_database_export.csv.crdownload")
		except:
			pass

		Serv.saveLog("sys", "Chrome -> Download error.")
		# 建立mail內容
		m = str(Utils.getTime()[1]) + " Chrome -> Download error."
		m += "\n" *2
		m += "Error Msg：%s\n"%(str(erMsg)) + "\n"
		m += "\n" *3
		m += "This mail is automatically sent by system, do not reply!  Thanks for your cooperation!"
		Utils.failedMail(m)
		exit()


# ========================================================== #
# 					 1. 先做自家資料庫 Start 					 #
# ========================================================== #

	VSLs, datas = Cleaner.Data2DB(df); Serv.saveLog("data", "Clear data for Database down.")

	# 多串入FromFile欄位
	datas = [d+[newPath] for d in datas]
	xlsIRCS = [str(d[4]).strip() for d in datas]
	xlsIMO = [str(d[2]).strip() for d in datas]
	xlsName = [regularVSLName(v) for v in VSLs]

	# 撈出現有資料
	Conn.Open()
	[rows, comm, _] = Conn.Select(
			table = ["VesselData"],
			columns = tuple([
				"VID", "WIN", "IMO", "RegsNum", "IRCS",
				"Flag", "VSLName1",
				"OwnrName", "OwnrAddr",
				"VSLType", "BuiltYear",
				"VSLCpcy", "VSLCpcyUnit",
				"AuthFrom", "AuthTo",
				"_State"
			]),
			conditions = "ORG='WCPFC'"
		)
	row = rows.fetchall()
	Conn.Close()
	dbIRCS  = []
	dbIMO   = []
	dbVSLNM = []


	# 1-1. 從網頁資料核對Excel
	Serv.saveLog("data", "Compare data from Database to Excel")
	dbExceptData = []
	delData = []
	updData = []

	updState = lambda IRCS, IMO, VSLNM, ST: Utils.updVesselData(CallSign=str(IRCS), IMO=str(IMO), VSLName=str(VSLNM), cols=["_State", "Origin"], vals=[str(ST), str(newPath)])

	try:
		for k, r in enumerate(row):
			dbIRCS.append(str(r[4]).strip())
			dbIMO.append(str(r[2]).strip())
			dbVSLNM.append(regularVSLName(r[6]))

			# 如果網頁資料不在Excel中，代表已刪除，修改_State欄位為0，並記錄資料來源
			# 先核對CallSign
			if str(r[4]).strip() != "":
				if str(r[4]).strip() not in xlsIRCS and str(r[-1]) == "1":
					# print("upd001", r)
					updState(r[4], r[2], r[6], "0")
					delData.append(r)

				# 如果CallSign在Excel中
				# elif str(r[4]).strip() in xlsIRCS and str(r[-1]) == "0":
				elif str(r[4]).strip() in xlsIRCS:
					# 再多確認IMO與船名預防重覆資料
					chkIRCS = [[v, xlsIMO[k], xlsName[k]] for k, v in enumerate(xlsIRCS) if str(r[4]).strip() == v]
					# 如果有同樣的CallSign出現
					if len(chkIRCS) > 1:
						# 判斷改0或改1
						# 如果IMO和船名和抓到的不一樣
						if not [True if all([r[2] == k[1], r[6] == k[2]]) else False for k in chkIRCS][0]:
							# print("upd002", r)
							if str(r[-1]) == "0":
								updState(r[4], r[2], r[6], "1")

					# 如果只有一個CallSign出現且狀態為0，就改1
					elif str(r[-1]) == "0":
						# print("upd003", r)
						updState(r[4], r[2], r[6], "1")
						updData.append(r)
				else:
					continue

			# 接著核對IMO
			elif str(r[2]).strip() != "":
				if r[2] not in xlsIMO and str(r[-1]) == "1":
					# print("upd004", r)
					updState(r[4], r[2], r[6], "0")
					delData.append(r)
				elif str(r[2]).strip() in xlsIMO and str(r[-1]) == "0":
					# print("upd005", r)
					updState(r[4], r[2], r[6], "1")
					updData.append(r)
				else:
					continue

			# 再核對船名
			elif regularVSLName(r[6]) != "":
				if regularVSLName(r[6]) not in xlsName and str(r[-1]) == "1":
					# print("upd006", r)
					updState(r[4], r[2], r[6], "0")
					delData.append(r)
				elif regularVSLName(r[6]) in xlsName and str(r[-1]) == "0":
					# print("upd007", r)
					updState(r[4], r[2], r[6], "1")
					updData.append(r)
				else:
					continue
			else:
				dbExceptData.append(r)

	except Exception as e:
		Serv.saveLog("sys", "DB -> Excel 執行失敗.")
		# 建立mail內容
		m = str(Utils.getTime()[1]) + " DB -> Excel Execute failure"
		m += "\n" *2
		m += "Error Msg：%s\n"%(str(e)) + "\n"
		m += "\n" *3
		m += "This mail is automatically sent by system, do not reply!  Thanks for your cooperation!"
		Utils.failedMail(m)
		exit()
	else:
		if dbExceptData:
			fn = str(dt.today().strftime("%Y%m%d"))+".txt"
			with open(f"D:/Crawler/Except/{fn}", "a", encoding="utf-8") as wf:
				wf.write("\n" + "==="*5 + "DB -> Excel" + "==="*5 + "\n")
				for e in dbExceptData:
					wf.write(str(e) + "\n")

	# 1-2. 從Excel核對網頁資料
	Serv.saveLog("data", "Compare data from Excel to Database")
	xlsExceptData = []
	istData = []
	try:
		# 先跑CallSign
		for k, d in enumerate(xlsIRCS):
			IRCS = str(d).strip()
			IMO = str(xlsIMO[k]).strip()
			VSLName = str(xlsName[k]).strip()
			# 如果任一個Excel資料非空值，且全不在網頁中，代表要新增
			# 反之核對是否有異動
			if any([IRCS != "", IMO != "", VSLName != ""]):
				# 20211209 修改執行insert的條件
				istFlag = False
				if IRCS not in dbIRCS and IRCS != "":
					istFlag = True
				elif IMO not in dbIMO and IMO != "":
					istFlag = True
				elif VSLName not in dbVSLNM and VSLName != "":
					istFlag = True

				if istFlag:
					val = datas[k]
					vals = [
						val[0], val[1], val[2], val[3], val[4], val[5],
						"WCPFC", VSLs[k], val[6], val[7], val[8], val[9],
						val[10], val[11], val[12], val[13], "1", val[15],
						# 20210910新增
						val[16],
					]
					istData.append(vals)
				else:
					if IRCS != "" and IRCS in dbIRCS:
						updData = Utils.cprVesselData(updData, datas[k], row[dbIRCS.index(IRCS)])
					elif IMO != "" and IMO in dbIMO:
						updData = Utils.cprVesselData(updData, datas[k], row[dbIMO.index(IMO)])
					elif VSLName != "" and VSLName in dbVSLNM:
						updData = Utils.cprVesselData(updData, datas[k], row[dbVSLNM.index(VSLName)])
					continue
			else:
				xlsExceptData.append(datas[k])
				continue
		else:
			if istData:
				# print("Insert =>", istData)
				Utils.istVesselData(vals = istData)
	except Exception as e:
		Serv.saveLog("sys", "Excel -> DB 執行失敗")
		# 建立mail內容
		m = str(Utils.getTime()[1]) + " Excel -> DB Execute failure"
		m += "\n" *2
		m += "Error msg：%s\n"%(str(e)) + "\n"
		m += "\n" *3
		m += "This mail is automatically sent by system, do not reply!  Thanks for your cooperation!"
		# print(m)
		Utils.failedMail(m)
		exit()
	else:
		if xlsExceptData:
			fn = str(dt.today().strftime("%Y%m%d"))
			with open("D:/Crawler/Except/{}.txt".format(fn), "a", encoding="utf-8") as wf:
				wf.write("\n" + "==="*5 + "Excel -> DB" + "==="*5 + "\n")
				for e in xlsExceptData:
					wf.write(str(e) + "n")



	# 1-3. 建立mail內容
	m = str(Utils.getTime()[1]) + " Following result as below:"
	m += "\n" *2
	if len(istData) == 0:
		m += "Insert: %s"%(str(len(istData))) + "\n"
	else:
		m += "Insert: %s"%(str(len(istData))) + " -> " + "、".join([v[0] for v in istData]) + "\n"
	m += "Modify: %s"%(str(len(updData))) + "\n"
	m += "Delete: %s"%(str(len(delData))) + "\n"
	m += "Except: %s"%(str(len(xlsExceptData) + len(dbExceptData))) + "\n"
	m += "\n" *3
	m += "This mail is automatically sent by system, do not reply!  Thanks for your cooperation!"
	m += "\n" *3
	# print(m)

	frnm = "OCR Server" # 寄件者名稱
	sender = "OCR@mail.idv.tw" # 寄件者信箱
	if testMode:
		tonm = "Ray"; receivers = ["ray@mail.idv.tw"] # 收件者清單 / 收件者信箱(測試)
		sbj = f"【Test】 WCPFC {str(Utils.getTime()[1])} Update Successful" # 主旨(測試)
	else:
		tonm = "Ray"; receivers = ["ray@mail.idv.tw"] # 收件者清單 / 收件者信箱
		sbj = "【Notice】 WCPFC {} Update Successful".format(str(Utils.getTime()[1])) # 主旨
	if dbExceptData or xlsExceptData:
		exceptPath = f"D:/Crawler/Except/{str(dt.today().strftime("%Y%m%d"))}.txt"
		mail.sentMail(fr=sender, frnm=frnm, to=receivers, tonm=tonm, cc="", sbj=sbj, m=m, attFile=[newPath, exceptPath])
	else:
		mail.sentMail(fr=sender, frnm=frnm, to=receivers, tonm=tonm, cc="", sbj=sbj, m=m, attFile=[newPath])
	Serv.saveLog("sys", "Sent mail: %s"%(sbj))

# ========================================================== #
# 					 1. 先做自家資料庫 End 					 #
# ========================================================== #
#######################   我是分隔線   ########################
# ========================================================== #
# 					 2. 做JDE的資料庫 Start 					 #
# ========================================================== #

	# 特殊船名不做任何事
	speName = [
		"SALAIÑO", "SALAI?O", "VALMITÃO", "VALMIT?O", "LBSTO.NIÑO-818", "LBSTO.NI?O-818", "STO.NINO-89", "STO.NI?O-89",
		"LBSTO.NIÑO-808", "LBSTO.NI?O-808", "STO.NINO-888", "STO.NI?O-888", "HONGFUNO.68", "HONGFUNO.68?",
		"KAISEI　NO.7", "KAISEI?？NO.7", "ELNINO", "ELNI?O", "LBNIÑO-5", "LBNI?O-5", "KAISEI　NO.2", "KAISEI?？NO.2",
		"??0?？DHIZON", "??9?？TAO", "DAFANO.168?", "YEONGSHUENNTSAIRNO.18?", "JINSINGSHYANGNO.1", "??9?？OTOMARU",
		"??0?？YOUKI", "NO.109?？YOKURYU", "NO.93?？TENKU", "??7?？KYOUHIME", "??3?？NOZOMIN", "NO.90?？HISYO"
	]

	# 2-1. 先找出當期資料
	# JDEdata(CallSign:[5], IMO:[6], VSLName:[3], VSLName(NoSp):[4], Remark:[13])
	JDEdata = JDE.selectCurrentData()
	if JDEdata[0]:
		JDEdata = JDEdata[1]
	else:
		if testMode:
			print(JDEdata[1])
		else:
			Serv.saveLog("sys", "Excel -> JDE Select執行失敗")
			JDE.failedMail_Excel2JDE(JDEdata[1])
			exit()
	Serv.saveLog("conn", "Query data from JDE")
	# JDE_IRCS = [r[5] for r in JDEdata] # 單獨拉出CallSign
	JDE_IRCS_NoSp = [str(r[5]).strip() for r in JDEdata] # 單獨拉出CallSign(做去空白處理)
	# JDE_IMO = [r[6] for r in JDEdata] # 單獨拉出IMO
	JDE_IMO_NoSp = [str(r[6]).strip() for r in JDEdata] # 單獨拉出IMO(做去空白處理)
	JDE_VSLName = [regularVSLName(r[3]).strip()[:20] for r in JDEdata] # 單獨拉出VesselName

	# Data2JDE回傳資料(VesselName, CallSign, IMO)
	xlsData = Cleaner.Data2JDE(df); Serv.saveLog("data", "Clear data for JDE down.")
	xls_IRCS_NoSp = []
	for r in xlsData[1]: # Excel去空白的CallSign
		if type(r) == dt:
			r = r.strftime("%d-%b")
			if r[0] == "0":
				r = r[1:]
		xls_IRCS_NoSp.append(str(r).strip())
	xls_IMO_NoSp = [str(r).strip() for r in xlsData[2]] # Excel去空白的IMO
	xls_VSLName_NoSp = [JDE.speCodeVSLName(regularVSLName(v)) for v in xlsData[0]] # Excel去空白轉大寫的船名
	xls_AuthDate = xlsData[3] # 白名單效期


	# 2-2-1. Excel比對JDE
	Serv.saveLog("data", "Compare data from Excel to JDE")
	JDE_insertData = [] # 初始化要塞進JDE的val串列
	xlsExceptData = []
	TABLENAME_UK04, _, _ = JDE.selectTimeSection()
	try:
		for k, cs in enumerate(xlsData[1]):
			tmp_cs = str(cs).strip()
			tmp_imo = str(xlsData[2][k]).strip()

			# 2-2-1-1. 從CallSign開始比對
			if tmp_cs != "": # 如果CallSign非空白
				if tmp_cs not in JDE_IRCS_NoSp:
					# 新增
					JDE_insertData.append([TABLENAME_UK04, cs, xlsData[2][k], xlsData[0][k], JDE.speCodeVSLName(regularVSLName(xlsData[0][k])), xls_AuthDate[k], "WCPFC", "Y", "Ray", "CWWCPFC"])
				else:
					continue

			# 2-2-1-2. 如果CallSign空白，比對IMO
			elif tmp_imo != "": # 如果IMO非空白
				if tmp_imo not in JDE_IMO_NoSp:
					# 新增
					JDE_insertData.append([TABLENAME_UK04, "", xlsData[2][k], xlsData[0][k], JDE.speCodeVSLName(regularVSLName(xlsData[0][k])), xls_AuthDate[k], "WCPFC", "Y", "Ray", "CWWCPFC"])
				else:
					continue

			# 2-2-1-3. 最後一關，如果Excel船名不在JDE資料中
			elif xls_VSLName_NoSp[k] != "": # 如果船名非空白
				# 如果去空白的船名(前20字元)不在JDE去空白的船名中
				if xls_VSLName_NoSp[k][:20] not in JDE_VSLName:
					# 新增
					JDE_insertData.append([TABLENAME_UK04, "", "", xlsData[0][k], JDE.speCodeVSLName(regularVSLName(xlsData[0][k])), xls_AuthDate[k], "WCPFC", "Y", "Ray", "CWWCPFC"])

			# 2-2-1-4. 整理出三個都空白的
			else:
				xlsExceptData.append([cs, xlsData[2][k], xlsData[0][k]])
				# print("xls Err => ", [xlsData[i][k] for i in range(3)])
		jdeIstRst = JDE.insertData(JDE_insertData)
		if jdeIstRst != True and not testMode:
			Serv.saveLog("sys", "Excel -> JDE Insert執行失敗")
			JDE.failedMail_Excel2JDE(jdeIstRst)
			exit()

	except Exception as e:
		# raise e
		Serv.saveLog("sys", "Excel -> JDE 執行失敗")
		JDE.failedMail_Excel2JDE(e)
		exit()
	else:
		if xlsExceptData:
			fn = str(dt.today().strftime("%Y%m%d"))
			with open(f"D:/Crawler/Except/{fn}.txt", "a", encoding="utf-8") as wf:
				wf.write("\n" + "==="*5 + "Excel -> JDE" + "==="*5 + "\n")
				for e in xlsExceptData:
					wf.write(str(e) + "n")

	# 2-2-2. JDE比對Excel
	Serv.saveLog("data", "Compare data from JDE to Excel")
	jdeExceptData = []
	JDE_updateData = []
	JDE_deleteData = []
	try:
		for k, d in enumerate(JDEdata):
			# 遇到備註(VR02)有值，就跳過
			if str(d[13]).strip() != "":
				continue

			IRCS = d[5].strip() if d[5] else ""
			IMO = d[6].strip() if d[6] else ""
			VSLName = d[3].strip() if d[3] else ""
			tmp_VSLName = regularVSLName(VSLName)

			# 帶特殊符號的船名略過
			if tmp_VSLName in speName:
				continue

			# 2-2-2-1. 如果IRCS非空白
			if IRCS != "":
				# 且不存在於Excel之中
				if IRCS not in xls_IRCS_NoSp and IRCS not in SpeKey:
					# 改狀態
					# print("jde upd001", d)
					JDE.updateData(EV01 = "N", cdn = f"IDTAX LIKE '{IRCS}%'")
					JDE_deleteData.append(True)
				elif IRCS in xls_IRCS_NoSp and IRCS not in SpeKey:
					# 如果IRCS非空白且在Excel之中，檢查IMO與船名
					IDX = xlsData[1].index(IRCS)
					if IMO not in xls_IMO_NoSp:
						xls_tmp_IMO = xlsData[2][IDX]
						# print("jde upd002", d)
						JDE.updateData(IMO = xls_tmp_IMO, AuthDate = xls_AuthDate[IDX], cdn = f"IDTAX LIKE '{IRCS}%' AND IDVR01 LIKE '{IMO}%'")
						JDE_updateData.append(True)

					if tmp_VSLName[:20] not in [k[:20] for k in xls_VSLName_NoSp]:
						xls_tmp_VSLName = xlsData[0][IDX]
						# print("jde upd003", d)
						if "'" in tmp_VSLName:
							tmp_VSLName = tmp_VSLName.replace("'", "''")
						JDE.updateData(VSLName = xls_tmp_VSLName, AuthDate = xls_AuthDate[IDX], cdn = f"IDTAX LIKE '{IRCS}%' AND IDALKY LIKE '{tmp_VSLName}%'")
						JDE_updateData.append(True)
				else:
					continue

			# 2-2-2-2. 如果IMO非空白
			elif IMO != "":
				# 且不存在於Excel之中
				if IMO not in xls_IMO_NoSp and IMO not in SpeKey:
					# 改狀態
					# print("jde upd004", d)
					JDE.updateData(EV01 = "N", cdn = f"IDEV01 LIKE '{IMO}%'")
					JDE_deleteData.append(True)
				elif IMO in xls_IMO_NoSp and IMO not in SpeKey:
					# 如果IMO非空白且在Excel之中，檢查CallSign與船名
					if IRCS not in xls_IRCS_NoSp:
						IDX = xlsData[2].index(IMO)
						xls_tmp_ICRS = xlsData[1][IDX]
						JDE.updateData(CallSign = xls_tmp_ICRS, AuthDate = xls_AuthDate[IDX], cdn = f"IDVR01 LIKE '{IMO}%' AND IDTAX LIKE '{IRCS}%'")
						JDE_updateData.append(True)

					if tmp_VSLName[:20] not in [k[:20] for k in xls_VSLName_NoSp]:
						IDX = xlsData[1].index(IRCS)
						xls_tmp_VSLName = xlsData[0][IDX]
						if "'" in tmp_VSLName:
							tmp_VSLName = tmp_VSLName.replace("'", "''")
						JDE.updateData(VSLName = xls_tmp_VSLName, AuthDate = xls_AuthDate[IDX], cdn = f"IDVR01 LIKE '{IMO}%' AND IDALKY LIKE '{tmp_VSLName}%'")
						JDE_updateData.append(True)
				else:
					continue

			# 2-2-2-3. 如果船名非空白
			elif tmp_VSLName != "":
				# 且不存在於Excel之中
				if tmp_VSLName[:20] not in [k[:20] for k in xls_VSLName_NoSp] and tmp_VSLName not in SpeKey:
					# 改狀態
					if str(d[4]).strip() != "":
						if "'" in tmp_VSLName:
							tmp_VSLName = tmp_VSLName.replace("'", "''")
						JDE.updateData(EV01 = "N", cdn = f"IDALKY LIKE '{tmp_VSLName}%'")
						JDE_deleteData.append(True)
					else:
						if "'" in tmp_VSLName:
							tmp_VSLName = tmp_VSLName.replace("'", "''")
						JDE.updateData(EV01 = "N", cdn = f"IDALPH LIKE '{tmp_VSLName}%'")
						JDE_deleteData.append(True)
				elif tmp_VSLName[:20] in [k[:20] for k in xls_VSLName_NoSp] and tmp_VSLName not in SpeKey:
					# 如果船名非空白且在Excel之中，檢查CallSign與IMO
					if IRCS not in xls_IRCS_NoSp:
						IDX = xlsData[0].index(VSLName)
						xls_tmp_ICRS = xlsData[1][IDX]
						if str(d[4]).strip() != "":
							if "'" in tmp_VSLName:
								tmp_VSLName = tmp_VSLName.replace("'", "''")
							JDE.updateData(CallSign = xls_tmp_ICRS, AuthDate = xls_AuthDate[IDX], cdn = f"IDALKY LIKE '{tmp_VSLName}%' AND IDTAX LIKE '{IRCS}%'")
							JDE_updateData.append(True)
						else:
							if "'" in tmp_VSLName:
								tmp_VSLName = tmp_VSLName.replace("'", "''")
							JDE.updateData(CallSign = xls_tmp_ICRS, AuthDate = xls_AuthDate[IDX], cdn = f"IDALPH LIKE '{tmp_VSLName}%' AND IDTAX LIKE '{IRCS}%'")
							JDE_updateData.append(True)

					if IMO not in xls_IMO_NoSp:
						IDX = xlsData[0].index(VSLName)
						xls_tmp_IMO = xlsData[1][IDX]
						if str(d[4]).strip() != "":
							if "'" in tmp_VSLName:
								tmp_VSLName = tmp_VSLName.replace("'", "''")
							JDE.updateData(IMO = xls_tmp_IMO, AuthDate = xls_AuthDate[IDX], cdn = f"IDALKY LIKE '{tmp_VSLName}%' IDVR01 LIKE '{IMO}%'")
							JDE_updateData.append(True)
						else:
							if "'" in tmp_VSLName:
								tmp_VSLName = tmp_VSLName.replace("'", "''")
							JDE.updateData(IMO = xls_tmp_IMO, AuthDate = xls_AuthDate[IDX], cdn = f"IDALPH LIKE '{tmp_VSLName}%' IDVR01 LIKE '{IMO}%'")
							JDE_updateData.append(True)
				else:
					continue

			# 2-2-2-4. 皆空白
			else:
				# 收集異常
				jdeExceptData.append(d)

	except Exception as e:
		# raise e
		Serv.saveLog("sys", "JDE -> Excel 執行失敗")
		JDE.failedMail_JDE2Excel(e)
		exit()
	else:
		if jdeExceptData:
			fn = str(dt.today().strftime("%Y%m%d"))
			with open(f"D:/Crawler/Except/{fn}.txt", "a", encoding="utf-8") as wf:
				wf.write("\n" + "==="*5 + "JDE -> Excel" + "==="*5 + "\n")
				for e in jdeExceptData:
					wf.write(str(e) + "n")

	# 2-2-3. 更新TABLENAME起迄日
	JDE.updateTimeSection()

	# 2-2-4. 建立mail內容
	m = str(Utils.getTime()[1]) + " Following result as below:"
	m += "\n" *2
	m += "Insert: %s"%(str(len(JDE_insertData))) + "\n"
	m += "Modify: %s"%(str(len(JDE_updateData))) + "\n"
	m += "Delete: %s"%(str(len(JDE_deleteData))) + "\n"
	m += "Except: %s"%(str(len(xlsExceptData) + len(jdeExceptData))) + "\n"
	m += "\n" *3
	m += "This mail is automatically sent by system, do not reply!  Thanks for your cooperation!"
	m += "\n" *3
	# print(m)

	frnm = "OCR Server"; sender = "OCR@mail.idv.tw" # 寄件者名稱 / 寄件者信箱
	if testMode:
		tonm = "Ray"; receivers = ["ray@mail.idv.tw"] # 收件者清單 / 收件者信箱(測試)
		ccnm = "Ray"; cc = ["ray@mail.idv.tw"]
		sbj = f"【Test】 WCPFC {str(Utils.getTime()[1])} Update Successful" # 主旨(測試)
	else:
		tonm = "Ray"; receivers = ["ray@mail.idv.tw"] # 收件者清單 / 收件者信箱
		ccnm = "Ray"; cc = ["ray@mail.idv.tw"]
		sbj = f"【Notice】 WCPFC {str(Utils.getTime()[1])} Update Successful" # 主旨
	if xlsExceptData or jdeExceptData:
		exceptPath = f"D:/Crawler/Except/{str(dt.today().strftime("%Y%m%d"))}.txt"
		mail.sentMail(fr=sender, frnm=frnm, to=receivers, tonm=tonm, cc=cc, ccnm=ccnm, sbj=sbj, m=m, attFile=[newPath, exceptPath])
	else:
		# pass
		mail.sentMail(fr=sender, frnm=frnm, to=receivers, tonm=tonm, cc=cc, ccnm=ccnm, sbj=sbj, m=m, attFile=[newPath])
	Serv.saveLog("sys", "Sent mail: %s"%(sbj))


# ========================================================== #
# 					 2. 做JDE的資料庫 End 					 #
# ========================================================== #

	Serv.saveLog("sys", "Crawler Successed. Cost -> %ss"%(str(int(time.time() - ts))))
	exit()

else:
	# 建立mail內容
	m = str(Utils.getTime()[1]) + " Executing Crawler Failure"
	m += "\n" *2
	m += "%s\n"%(str(CW_rst[1])) + "\n"
	m += "\n" *3
	m += "This mail is automatically sent by system, do not reply!  Thanks for your cooperation!"
	m += "\n" *3

	frnm = "OCR Server"; sender = "OCR@mail.idv.tw" # 寄件者名稱 / 寄件者信箱
	if testMode:
		tonm = "Ray"; receivers = ["ray@mail.idv.tw"] # 收件者清單 / 收件者信箱(測試)
		ccnm = "Ray"; cc = ["ray@mail.idv.tw"]
		sbj = f"【Test】 WCPFC {str(Utils.getTime()[1])} Failure" # 主旨(測試)
	else:
		tonm = "Ray"; receivers = ["ray@mail.idv.tw"] # 收件者清單 / 收件者信箱
		ccnm = "Ray"; cc = ["ray@mail.idv.tw"]
		sbj = f"【Notice】 WCPFC {str(Utils.getTime()[1])} Failure" # 主旨
	mail.sentMail(fr=sender, frnm=frnm, to=receivers, tonm=tonm, cc=cc, ccnm=ccnm, sbj=sbj, m=m)
	Serv.saveLog("sys", "Crawler Failed. Cost -> %ss"%(str(int(time.time() - ts))))
	exit()