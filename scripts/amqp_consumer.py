#!/usr/bin/env python
# encoding: utf-8
import time
import simplejson 
import string
import getopt
import amqp_consumer_sub
import hadoop_mailer
import os
from amqplib import client_0_8 as amqp

conn = amqp.Connection(host="hive1:5672", userid="guest", password="guest", virtual_host="/", insist=False)
chan = conn.channel()

chan.queue_declare(queue="po_box", durable=True, exclusive=False, auto_delete=False)
chan.exchange_declare(exchange="amq.direct", type="direct", durable=True, auto_delete=False,)

chan.queue_bind(queue="po_box", exchange="amq.direct", routing_key="testRoute")

def recv_callback(msg):
	print '\nReceived time: ' + time.strftime("%Y-%m-%d-%H:%M:%S", time.localtime()) + '\n'
	print 'Received: ' + msg.body + '\n'
	try:
	  params = simplejson.loads(msg.body)
	  emailaddr = params["email"]
	  print "emailaddr =  " + emailaddr + "\n"
	except:
	  err_msg = "Oops! Error for retreive email!"
	  print err_msg 
	rec_msg = "\nReceived the request.\nHive will execute the request when it is its turn."  
	try:
		email("Received the request","received",os.uname()[1],emailaddr,rec_msg)
	except:
		err_msg = "The Email Server has Raised an Error."
		print err_msg
	try:
		path = os.environ.get("ANALYTICS_LIB") 
                if path is None or path == "":
                    path = "/home/hadoop/analytics/library"
                path += "/scripts/amqp_consumer_sub.py"
		args = ['amqp_consumer_sub.py', msg.body]
		print "MSGBODY====>:  " + msg.body + "\n"
 		pid = os.fork()         # Create child
        	if pid == 0:            # child
			os.execv(path, args) 
	except ValueError:
  		err_msg = "Oops!  The passed parameter list has some errors."
  		print err_msg 
        	email("Invalid parameter list","failed",os.uname()[1],emailaddr,err_msg)
	except:
        	err_msg = "Oops! Unexpected Error."
        	print err_msg
        	email("Invalid parameter list","failed",os.uname()[1],emailaddr,err_msg)

def email(subject, status, from_domain="localhost", to_email="", msg=""):
        if to_email is not None and to_email.strip() != '':
                hadoop_mailer.main(['hadoop_mailer.py', subject, status, from_domain, to_email, msg])
        return

chan.basic_consume(queue='po_box', no_ack=True, callback=recv_callback, consumer_tag="testtag")
while True:
    chan.wait()
chan.basic_cancel("testtag")


chan.close()
conn.close()

