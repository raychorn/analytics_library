#!/usr/bin/env python
# encoding: utf-8
"""
hadoop_mailer.py

Usage:

python hadoop_mailer.py run_daily_timelines.sh starting vzmm.analytics.smithmicro.com Analytics.Notifications@smithmicro.com [[MSG(body)] | [{'text':'', 'html':'', 'version':'', 'cluster':''}]]
python hadoop_mailer.py run_daily_timelines.sh complete connectivity.analytics.smithmicro.com Analytics.Notifications@smithmicro.com [[MSG(body)] | [{'text':'', 'html':'', 'version':'', 'cluster':''}]]
python hadoop_mailer.py [SUBJECT] [starting|complete] [from(domain)] [to(email@address)] [[MSG(body)] | [\"{'text':'', 'html':'', 'version':'', 'cluster':''}\"]]

"""

import os
import sys
import smtplib
import datetime
import string
try:
	import ast
except ImportError:
	import compiler
	
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.MIMEImage import MIMEImage

dir = os.path.abspath(os.path.dirname(sys.argv[0]))

def main(argv=None):
	if argv is None:
	  argv = sys.argv
	print argv
	# Check how many arguments were passed, minimum is 4
	if len(argv) <= 4:
	  print "Usage: python hadoop_mailer.py [SUBJECT] [starting|complete] [from(domain)] [to(email@address)] [[MSG(body)] | [\"{'text':'', 'html':'', 'version':'', 'cluster':''}\"]]"
	  sys.exit(0)

	# Define Email and Server Variables
	SERVER = "localhost"
	#SERVER = "207.67.226.5"
	FROM = "analytics@%s" % argv[3]
	TO = ["%s" % argv[4]] # must be a list
	SUBJECT = "Analytics %s is %s" % (argv[1], argv[2])

	# msgRoot will be sent to smtp.
	msgRoot = MIMEMultipart('related')
	msgRoot['Subject'] = SUBJECT
	msgRoot['From'] = FROM
	msgRoot['To'] = ", ".join(TO)
	msgRoot.preamble = 'This is a multi-part message in MIME format.'
	
	print "===>arg[0] = %s; arg[1]=%s; argv[2]=%s; argv[3]=%s; argv[4]=%s" %(argv[0], argv[1], argv[2], argv[3], argv[4]) 
	
	details = {}
	if len(argv) > 5:
		try:
			details = ast.literal_eval(argv[5])
		except:
			try:
				details = eval(argv[5]) #compiler.parse(argv[5],"eval")
			except SyntaxError, err:
				details['text'] = argv[5]
		
	#filename = str(fileDetails['filename']) if fileDetails.has_key('filename') else ''
	#fileurl = str(fileDetails['fileurl']) if fileDetails.has_key('fileurl') else ''
	#username = str(fileDetails['username']) if fileDetails.has_key('username') else ''
	#password = str(fileDetails['password']) if fileDetails.has_key('password') else ''
	version = str(details['version']) if details.has_key('version') else '1.1-r14321'
	cluster = str(details['cluster']) if details.has_key('cluster') else 'master'
	
	# Clean Text so that newlines do not appear in the email
	TEXT = ""
	#TEXT = TEXT + '\nTime is %s\n' % datetime.datetime.now()  
	if details.has_key('text'):
	  for item in (details['text'].split('\\n')):
	    TEXT = TEXT + item + '\n' + "\r\nSmithMicro Analytics Version: "+ version +" \r\nCluster: "+ cluster + ""
	
	# Encapsulate the plain and HTML versions of the message body in an
	# 'alternative' part, so message agents can decide which they want to display.
	msgAlternative = MIMEMultipart('alternative')
	msgRoot.attach(msgAlternative)
	
	# Text Version
	msgText = MIMEText(TEXT)
	msgAlternative.attach(msgText)
	
	# HTML Version
	# We reference the image in the IMG SRC attribute by the ID we give it below
	#msgText = MIMEText('<p>Your file is now complete. To access your report, click on this link: <a href=\"'+fileurl+'\">'+filename+'</a> \
	#and enter your username and password below.</p><p>Username: '+ username +'<br />Password: '+ password +'</p><br><img src="cid:logo"> \
	#<br /><br><span style="font-size:small;"><i>SmithMicro Analytics Version: '+ '1.1-r13669' +'</i><br /><i>Cluster: '+ cluster +'</i> \
	#<br /><i>Server Time: '+ str(datetime.datetime.now()) +'</i></span>', 'html')
	footerHtml = '<br><img src="cid:logo"><br /><br><span style="font-size:small;"><i>SmithMicro Analytics Version: '+ version +'</i><br /><i>Cluster: '+ cluster +'</i><br /><i>Server Time: '+ str(datetime.datetime.now()) +'</i></span>'
	if details.has_key('html'):
		msgText = MIMEText(details['html'] + footerHtml, 'html')
		msgAlternative.attach(msgText)
	
	# Open image for reading
	fp = open(dir + '/SMSI_Analytics_logo.jpg', 'rb')
	msgImage = MIMEImage(fp.read())
	fp.close()
	
	# Define the image's ID as referenced above
	msgImage.add_header('Content-ID', '<logo>')
	msgRoot.attach(msgImage)
	

	# Prepare actual message  
	message = string.join((
	    "From: %s" % FROM,
	    "To: %s" % ", ".join(TO),
	    "Subject: %s" % SUBJECT,
	    "",
	    TEXT + "\r\nVersions: [\"{'analytics_lib': 'trunk-r13540'}\"]"
	    ), "\r\n")

	# Send the mail
	server = smtplib.SMTP(SERVER)
	server.set_debuglevel(0)
	server.sendmail(FROM, TO, msgRoot.as_string())#message)
	server.quit()

if __name__ == "__main__":
	sys.exit(main())

