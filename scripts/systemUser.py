#!/usr/bin/env python
# encoding: utf-8
"""
systemUser.py

"""
import os
import sys
import optparse
import subprocess
try:
	# only in python 2.5
	import hashlib
	sha = hashlib.sha1
	md5 = hashlib.md5
	sha256 = hashlib.sha256
except ImportError:
	# fallback
	import sha
	import md5

__author__  = "Luis Ramos"
__email__   = "lramos@smithmicro.com"
__date__    = "2010-12-13"
__version__ = 0.2

import os, sys, crypt, string, getpass
from random import choice

#Analytics Library Directory
analytics_lib = str(os.getenv("ANALYTICS_LIB"))
def get_analytics_lib():
	analytics_lib = xstr(os.getenv("ANALYTICS_LIB"))
	if analytics_lib is None or analytics_lib.strip() == '':
		analytics_lib = "/home/hadoop/analytics/library"
	return analytics_lib

# Behaves like str() but instead returns empty string if s is None
def xstr(s):
	if s is None:
		return ''
	return str(s)

class password:
	MD5saltprefix                   = "$1$"
	DESsaltprefix                   = ""
	valid_salt_set                  = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ/."
	valid_password_set              = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!\"#$%&'()*+,-./:;<=>?@[]^_`{|}~\ "

	def gen_random(self, set, length):
		""" generate random string length long from set """
		random_string = ""
		for i in range(length):
			random_string = random_string + choice(set)
		return random_string

	def gen_MD5salt(self):
		""" generate a random MD5 salt eight long """
		return self.MD5saltprefix + self.gen_random(self.valid_salt_set, 8)

	def gen_DESsalt(self):
		""" generate a random DES salt two long """
		return self.DESsaltprefix + self.gen_random(self.valid_salt_set, 2)

	def gen_password(self, length):
		""" generate a random password string length long """
		return self.gen_random(self.valid_password_set, length)

	def cleartext(self):
		""" prompt for a password and return the string in clear text """
		return getpass.getpass()

	def encryptPrompt(self, salt):
		""" prompt for a password and return the string encrypted using salt """
		_cleartext = self.cleartext()
		return crypt.crypt(_cleartext, salt)
		
	def encrypt(self, password, salt):
		""" prompt for a password and return the string encrypted using salt """
		return crypt.crypt(password, salt)

def check(username):
	users = file('/etc/passwd','r').read()
	if username in users:
		return True
	return False

def remove():
	pass

def add(username, salt = 'password'):
	#Check if user exist before proceding
	modifiedUser = username + ""
	if check(username) is False:
		#Create/Generate Password and Encrypt It.
		md5pass = password().gen_MD5salt()
		encryptedPassword = password().encrypt(md5pass,salt)
		#Call system useradd command
		os.system("sudo /usr/sbin/useradd -m -k " + get_analytics_lib() + "/ftpusers/default" + " -s /usr/sbin/scponlyc -p '" + encryptedPassword + "' " + modifiedUser)
		os.system("sudo /bin/chgrp hadoop /home/" + modifiedUser + "/csv/")
		os.system("sudo /bin/chmod g+w /home/" + modifiedUser + "/csv/")
		#Save Password to a File to later retrive it.
		FILE = open(get_analytics_lib() + "/ftpusers/"+username,"w")
		FILE.writelines(md5pass + "\n")		
		#print "sudo /usr/sbin/useradd -m -d " + get_analytics_lib() + "/ftpusers/"+modifiedUser+ " -k " + get_analytics_lib() + "ftpusers/default" + " -s /bin/bash -p '" + encryptedPassword + "' " + modifiedUser
		return modifiedUser, md5pass
	else:
		md5pass = file(get_analytics_lib() + '/ftpusers/' + username,'r').read()
		return modifiedUser, md5pass
	return False

def main(argv=None):
	""" Main Application Handler """
	usage = "usage: %%prog [options]"
	version_string = "%%prog %s" % __version__
	description = "systemUser.py - Adds a User to the System (this requires the useradd linux command)"
	
	# Create our parser and setup our command line options
	parser = optparse.OptionParser(usage=usage, version=version_string, description=description)
	
	parser.add_option("-a", "--add", action="store", dest="add", default=False, help="Add a non-existing User")
	parser.add_option("-c", "--check", action="store", dest="check", default=False, help="Check if user has been created")
	
	# Parse our options and arguments   
	options, args = parser.parse_args()
	
	# Start Checking Parser Options:
	print "Options:"
	print options, args
	if options.add:
		print add(options.add)
	if options.check:
		print check(options.check)	


if __name__ == "__main__":
	sys.exit(main())
