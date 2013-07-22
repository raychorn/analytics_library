#!/usr/bin/env python   
import sys
import hmac
from binascii import hexlify, unhexlify
from struct import pack
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

# this is what you want to call.
def pbkdf2( password, salt, itercount, keylen, hashfn = sha ):
	try:
		# depending whether the hashfn is from hashlib or sha/md5
		digest_size = hashfn().digest_size
	except TypeError:
		digest_size = hashfn.digest_size
	# l - number of output blocks to produce
	l = keylen / digest_size
	if keylen % digest_size != 0:
		l += 1

	h = hmac.new( password, None, hashfn )

	T = ""
	for i in range(1, l+1):
		T += pbkdf2_F( h, salt, itercount, i )

	return T[0: keylen]

def xorstr( a, b ):
	if len(a) != len(b):
		raise "xorstr(): lengths differ"

	ret = ''
	for i in range(len(a)):
		ret += chr(ord(a[i]) ^ ord(b[i]))

	return ret

def prf( h, data ):
	hm = h.copy()
	hm.update( data )
	return hm.digest()

# Helper as per the spec. h is a hmac which has been created seeded with the
# password, it will be copy()ed and not modified.
def pbkdf2_F( h, salt, itercount, blocknum ):
	U = prf( h, salt + pack('>i',blocknum ) )
	T = U

	for i in range(2, itercount+1):
		U = prf( h, U )
		T = xorstr( T, U )

	return T

		
def test():	
	# from analytics
	# 2010-09-15
	password = '084811e2-ba2f-4aa9-9bf7-9ad989df91f2'
	salt = unhexlify('cb2fa5c5f963ee0d')
	expect = '6d9e59b2faeeab390b57e9f8d42412b214af4a650e32c317b756aa07e4112b2dd6de046d1e34fa2ddbecceac93547910'
	# 2010-11-12
	password = '3727d887-9528-4a7c-9e47-0b683aa635e6'
	salt = unhexlify('f28f867fd49d91d9')
	expect = '89f6010b7faa82dfb3aa11af3ca3444d712fe88de56209f86da2b23b211b55fd86b17b0289b37c4025a4b7534f3c6f9d'
	# False
	#password = '1b0565a5-53aa-4be2-8afb-00000000000'
	#salt = unhexlify('5810053f283745be')
	#expect = 'cc86e6ffba7b3b93b52b4c9b6cf5c5c14c3cd96ca63ad098fe9f95cd2e15904187753d6788dea25c668a5b4a9126cbce'
	itercount = 1000
	keylen = 48
	ret = pbkdf2( password, salt, itercount, keylen )
	hexret = hexlify(ret)
	print "key:      %s" % hexret
	print "expected: %s" % expect
	print "passed:   %s" % str(hexret == expect)

if __name__ == '__main__':
	test()
