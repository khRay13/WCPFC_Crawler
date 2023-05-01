from datetime import datetime as dt
from datetime import timedelta
import cx_Oracle as ora
from time import sleep

try:
	from mail import Mail
except:
	from .mail import Mail

testMode = False

class JDE:
	def __init__(self):
		self.SERVER = "127.0.0.1" # JDE Server IP
		self.PORT = "1521" # JDE Server Port
		self.UID = self.PWD ="UID&PWD"
		self.DBNM = "DBNAME"

		self.ymdhms = lambda t: t.strftime("%Y-%m-%d %H:%M:%S")
		self.julDay = lambda t: 100000 + int(str(t.year)[-2:]) * 1000 + int(t.strftime("%j")) # %j -> day of year
		self.mail = Mail()

	def Open(self):
		return ora.connect(self.UID, self.PWD, "{}/{}".format(self.SERVER, self.DBNM))

	def Close(self, curs, conn):
		curs.close()
		conn.close()

	def selectTimeSection(self):
		# 查詢目前的起迄日
		sql = "SELECT AA.IVUK04, AA.IVD1J, AA.IVD3J FROM proddta.TABLENAME AA WHERE ROWNUM =1 ORDER BY AA.IVUK04 DESC" # PD
		# sql = "SELECT IVUK04, IVD1J, IVD3J FROM TABLENAME WHERE ROWNUM =1 ORDER BY IVUK04 DESC" # PY
		conn = self.Open()
		cursor = conn.cursor()
		qry = cursor.execute(sql)
		rst = qry.fetchall()
		UK04, STD, ETD = rst[0]
		self.Close(cursor, conn)

		return UK04, STD, ETD

	def updateTimeSection(self):
		UK04, STD, ETD = self.selectTimeSection()

		# 製造新的起迄日
		tdy = dt.today()
		deltaDay = tdy + timedelta(days=1)
		nSTD = self.julDay(tdy)
		nETD = self.julDay(deltaDay)

		# TABLENAME
		if testMode:
			pass
		else:
			sql = f"UPDATE proddta.TABLENAME SET IVD1J = '{nSTD}', IVD3J = '{nETD}' WHERE IVUK04 = '{UK04}'" # PD
			# sql = f"UPDATE TABLENAME SET IVD1J = '{nSTD}', IVD3J = '{nETD}' WHERE IVUK04 = '{UK04}'" # PY

			conn = self.Open()
			cursor = conn.cursor()
			cursor.execute(sql)
			conn.commit()
			self.Close(cursor, conn)

	def selectUKID(self):
		sql = "SELECT AA.UKUKID FROM proddta.TABLENAME AA WHERE AA.UKOBNM = 'TABLENAME'" # 記編號的Table(PD環境)
		# sql = "SELECT IDUK05 FROM proddta.TABLENAME WHERE rownum=1 ORDER BY IDUK05 DESC" # 抓52A.UK05(手動調整編號使用)
		# sql = "SELECT UKUKID FROM TABLENAME WHERE UKOBNM = 'TABLENAME'" # 記編號的Table(PY環境)
		conn = self.Open()
		cursor = conn.cursor()
		qry = cursor.execute(sql)
		rst = qry.fetchall()
		self.Close(cursor, conn)

		return rst[0][0]

	def updateUKID(self, UKID):
		if testMode:
			pass
		else:
			sql = f"UPDATE proddta.TABLENAME SET UKUKID = '{UKID}' WHERE UKOBNM = 'TABLENAME'" # PD
			# sql = f"UPDATE TABLENAME SET UKUKID = '{UKID}' WHERE UKOBNM = 'TABLENAME'" # PY
			conn = self.Open()
			cursor = conn.cursor()
			cursor.execute(sql)
			conn.commit()
			self.Close(cursor, conn)

	def selectCurrentData(self):
		# qry_col = f"SELECT table_name, column_name FROM USER_TAB_COLUMNS WHERE table_name = '{TABLENAME}'" # 查詢 column name
		# sql = "SELECT * FROM TABLENAME"
		sql = "SELECT BB.* FROM proddta.TABLENAME AA INNER JOIN proddta.TABLENAME BB ON AA.IVUK04 = BB.IDUK04 WHERE AA.IVEV01 = '2' AND BB.IDEV01 = 'Y'" # 查詢資料(PD環境)
		# sql = "SELECT BB.* FROM TABLENAME AA INNER JOIN TABLENAME BB ON AA.IVUK04 = BB.IDUK04 WHERE AA.IVEV01 = '2' AND BB.IDEV01 = 'Y'" # 查詢資料(PY環境)
		# sql = f"SELECT ID{AN80} FROM TABLENAME WHERE IDDSC1 LIKE 'WCPFC%'"

		try:
			# conn1 = self.Open()
			# cursor1 = conn1.cursor()
			# qry1 = cursor1.execute(qry_col)
			# rst1 = qry1.fetchall()
			# self.Close(cursor1, conn1)

			conn2 = self.Open()
			cursor2 = conn2.cursor()
			qry2 = cursor2.execute(sql)
			rst2 = qry2.fetchall()
		except Exception as e:
			return [False, str(e)]
		else:
			# rst1 = [r[1] for r in rst1]
			# return [rst1, rst2]
			return [True, rst2]
		finally:
			self.Close(cursor2, conn2)

	def updateData(self, CallSign = None, IMO = None, VSLName = None, AuthDate = None, EV01 = "Y", USR = "Ray", PID = "CWWCPFCRAY", cdn = "1=1"):
		# IDTAX, IDVR01, IDALKY
		if testMode:
			pass
		else:
			sql = ""
			if EV01 != "Y":
				sql = f"UPDATE proddta.TABLENAME SET IDEV01 = '{EV01}', IDPID = '{PID}', IDUPMJ = '{self.julDay(dt.today())}' WHERE {cdn}" # PD
				# sql = f"UPDATE TABLENAME SET IDEV01 = '{EV01}', IDUSER = '{USR}', IDPID = '{PID}', IDUPMJ = '{self.julDay(dt.today())}' WHERE {cdn}" # PY
			else:
				if CallSign != None:
					sql = f"UPDATE proddta.TABLENAME SET IDTAX = '{CallSign}', IDVR02 = '{AuthDate}', IDEV01 = '{EV01}', IDPID = '{PID}', IDUPMJ = '{self.julDay(dt.today())}' WHERE {cdn}" # PD
					# sql = f"UPDATE TABLENAME SET IDTAX = '{CallSign}', IDVR02='{AuthDate}', IDEV01 = '{EV01}', IDUSER = '{USR}', IDPID = '{PID}', IDUPMJ = '{self.julDay(dt.today())}' WHERE {cdn}" # PY

				if IMO != None:
					sql = f"UPDATE proddta.TABLENAME SET IDVR01 = '{IMO}', IDVR02 = '{AuthDate}', IDEV01 = '{EV01}', IDPID = '{PID}', IDUPMJ = '{self.julDay(dt.today())}' WHERE {cdn}" # PD
					# sql = f"UPDATE TABLENAME SET IDVR01 = '{IMO}', IDVR02='{AuthDate}', IDEV01 = '{EV01}', IDUSER = '{USR}', IDPID = '{PID}', IDUPMJ = '{self.julDay(dt.today())}' WHERE {cdn}" # PY

				if VSLName != None:
					ALPH = str(VSLName)
					ALKY = str(VSLName).replace(" ", "").upper().strip()
					if len(ALPH) > 20: ALPH = ALPH[:20]
					if len(ALKY) > 20: ALKY = ALKY[:20]
					if "'" in ALPH:
						sql = f"UPDATE proddta.TABLENAME SET IDALPH = q'[{ALPH}]', IDALKY = q'[{ALKY}]', IDVR02 = '{AuthDate}', IDEV01 = '{EV01}', IDPID = '{PID}', IDUPMJ = '{self.julDay(dt.today())}' WHERE {cdn}" # PD
						# sql = f"UPDATE TABLENAME SET IDALPH = q'[{ALPH}]', IDALKY = q'[{ALKY}]', IDVR02 = '{AuthDate}', IDEV01 = '{EV01}', IDUSER = '{USR}', IDPID = '{PID}', IDUPMJ = '{self.julDay(dt.today())}' WHERE {cdn}" # PY
					else:
						sql = f"UPDATE proddta.TABLENAME SET IDALPH = '{ALPH}', IDALKY = '{ALKY}', IDVR02 = '{AuthDate}', IDEV01 = '{EV01}', IDPID = '{PID}', IDUPMJ = '{self.julDay(dt.today())}' WHERE {cdn}" # PD
						# sql = f"UPDATE TABLENAME SET IDALPH = '{ALPH}', IDALKY = '{ALKY}', IDVR02 = '{AuthDate}', IDEV01 = '{EV01}', IDUSER = '{USR}', IDPID = '{PID}', IDUPMJ = '{self.julDay(dt.today())}' WHERE {cdn}".format(, , , , , , , ) # PY
			# print(sql)
			conn = self.Open()
			cursor = conn.cursor()
			cursor.execute(sql)
			conn.commit()
			# print(sql)
			self.Close(cursor, conn)
			# sleep(0.1)


	def insertData(self, Datas):
		if testMode:
			pass
		else:
			try:
				skip = 100
				for k in range(0, len(Datas), skip):
					sql = ""
					for row in Datas[k:k+skip]:
						valRows = ""
						for val in row:
							valRows += "q'[" if "'" in str(val) else "'"
							valRows += str(val)
							valRows += "]'," if "'" in str(val) else "',"
						valRows += "'{}'".format(self.julDay(dt.today()))
						valRows += ","

						lastUK05 = self.selectUKID() # 1. 撈出F00022最新的UKID
						# print("S1:",lastUK05)
						lastUK05 += 1 # 2. UKID進一位
						# print("S2:",lastUK05)
						self.updateUKID(str(lastUK05)) # 3. 先回寫進位後的UKID到F00022
						sql = f"INSERT INTO proddta.TABLENAME(IDUK05, IDUK04, IDTAX, IDVR01, IDALPH, IDALKY, IDVR02, IDDSC1, IDEV01, IDUSER, IDPID, IDUPMJ) VALUES({str(lastUK05) + "," + valRows[:-1]})" # PD # 4. 之後資料再新增到52A
						# sql += f"INTO proddta.TABLENAME(IDUK05, IDUK04, IDTAX, IDVR01, IDALPH, IDALKY, IDVR02, IDDSC1, IDEV01, IDUSER, IDPID, IDUPMJ) VALUES{valRows[:-1]}" # PD
						# sql += f"INTO TABLENAME(IDUK05, IDUK04, IDTAX, IDVR01, IDALPH, IDALKY, IDVR02, IDDSC1, IDEV01, IDUSER, IDPID, IDUPMJ) VALUES{valRows[:-1]}" # PY

					# sql = "INSERT ALL " +sql + " SELECT 1 FROM proddta.DUAL" # PD
					# sql = "INSERT ALL " +sql + " SELECT 1 FROM DUAL" # PY

						# print(sql)
						conn = self.Open()
						cursor = conn.cursor()
						cursor.execute(sql)
						conn.commit()
						self.Close(cursor, conn)
						sleep(0.02)
			except Exception as e:
				return str(e)
			else:
				return True

	def speCodeVSLName(self, s):
		rpls = lambda cd, s_: cd.get(s_, s_)
		speCode = {"Ã":"A", "Ï":"I", "Ñ":"N"}
		if any([True for c in speCode if c in speCode]):
			return "".join([rpls(speCode, c) for c in s])
		else:
			return s


	def failedMail_Excel2JDE(self, e):
		# 建立mail內容
		m = str(self.ymdhms(dt.today())) + " Excel -> JDE 執行失敗"
		m += "\n" *2
		m += "Error msg：{}\n".format(str(e)) + "\n"
		m += "\n" *3
		m += "This mail is automatically sent by system, do not reply!  Thanks for your cooperation!"

		frnm = "OCR Server"; sender = "OCR@mail.idv.tw" # 寄件者名稱 / # 寄件者信箱
		if testMode:
			tonm = "Ray"; receivers = ["ray@mail.idv.tw"] # 收件者清單 / # 收件者信箱(測試)
			ccnm = "Ray"; cc = ["ray@mail.idv.tw"]
			sbj = "【Test】 WCPFC {} Update failure".format(str(self.ymdhms(dt.today()))) # 主旨(測試)
		else:
			tonm = "Ray"; receivers = ["ray@mail.idv.tw"] # 收件者清單 / 收件者信箱
			ccnm = "Ray"; cc = ["ray@mail.idv.tw"]
			sbj = "【Notice】 WCPFC {} Update failure".format(str(self.ymdhms(dt.today()))) # 主旨
		self.mail.sentMail(fr=sender, frnm=frnm, to=receivers, tonm=tonm, cc=cc, ccnm=ccnm, sbj=sbj, m=m)

	def failedMail_JDE2Excel(self, e):
		# 建立mail內容
		m = str(self.ymdhms(dt.today())) + " JDE -> Excel 執行失敗"
		m += "\n" *2
		m += "Error msg：{}\n".format(str(e)) + "\n"
		m += "\n" *3
		m += "This mail is automatically sent by system, do not reply!  Thanks for your cooperation!"

		frnm = "OCR Server"; sender = "OCR@mail.idv.tw" # 寄件者名稱 / # 寄件者信箱
		if testMode:
			tonm = "Ray"; receivers = ["ray@mail.idv.tw"] # 收件者清單 / # 收件者信箱(測試)
			ccnm = "Ray"; cc = ["ray@mail.idv.tw"]
			sbj = "【Test】 WCPFC {} Update failure".format(str(self.ymdhms(dt.today()))) # 主旨(測試)
		else:
			tonm = "Ray"; receivers = ["ray@mail.idv.tw"] # 收件者清單 / 收件者信箱
			ccnm = "Ray"; cc = ["ray@mail.idv.tw"]
			sbj = "【Notice】 WCPFC {} Update failure".format(str(self.ymdhms(dt.today()))) # 主旨
		self.mail.sentMail(fr=sender, frnm=frnm, to=receivers, tonm=tonm, cc=cc, ccnm=ccnm, sbj=sbj, m=m)

