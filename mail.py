import smtplib
from email.mime.text import MIMEText
from email.header import Header
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
class Mail:
	def __init__(self):
		self.SMTPServer = "127.0.0.1" # SMTP IP

	def sentMail(self, fr="", frnm="", to="", tonm="", cc="", ccnm="", sbj="", m="", attFile = None):
		msg = MIMEMultipart()
		msg["From"] = Header(frnm, "utf-8")
		msg["To"] = Header(tonm, "utf-8")
		msg["Subject"] = Header(sbj, "utf-8")

		s = MIMEText(m, "plain", "utf-8")
		msg.attach(s)

		if attFile != None:
			for f in attFile:
				att = MIMEApplication(open(f, "rb").read())
				att.add_header("Content-Disposition", "attachment", filename = f)
				msg.attach(att)

		smtpObj = smtplib.SMTP(self.SMTPServer)
		if cc != "":
			msg["Cc"] = Header(ccnm, "utf-8")
			_ = smtpObj.sendmail(fr, to + cc, msg.as_string())
		else:
			_ = smtpObj.sendmail(fr, to, msg.as_string())
		smtpObj.quit()