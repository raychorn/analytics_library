import hashlib
import hmac
from hmac import HMAC
print "--HASH TEST--"
'''print "--------------------------"
print "Testing MD5:"
m = hashlib.md5()
m.update("Nobody inspects")
m.update(" the spammish repetition")
print m.hexdigest()
print "size: " + str(m.digest_size)
print "--------------------------"
'''
print "--------------------------"
print "Testing SHA-1:"
print hashlib.sha1("Nobody inspects the spammish repetition").hexdigest()
print "--------------------------"

def pbkdf_sha1(password, salt, iterations):
  result = password
  for i in xrange(iterations):
    result = HMAC(result, salt, hashlib.sha1).hexdigest() # use HMAC to apply the salt
  return result

def check_password(hashed_password, plain_password):
  hashed_password = hashed_password.decode("base64")
  return hashed_password == plain_password

#Variables
iter = 1000
key_len = 48
guid = "084811e2-ba2f-4aa9-9bf7-9ad989df91f2"
token = "cb2fa5c5f963ee0d6d9e59b2faeeab390b57e9f8d42412b214af4a650e32c317b756aa07e4112b2dd6de046d1e34fa2ddbecceac93547910"
salt = token[:16]
hash = token[16:]

#Print Information
print "Security Check:"
print "GUID: " + guid
print "TOKEN: " + token
print "SALT: " + salt
print "HASH: " + hash

result = pbkdf_sha1(guid,salt,iter) #.encode("base64").strip() 
print "RESULT: " + result
print check_password(result, hash)
print hmac.new(guid, salt, hashlib.sha1).hexdigest()
