#!/usr/bin/python
import salt.config
import salt.crypt
import binascii

opts = salt.config.master_config('/etc/salt/master')
keys = salt.crypt.MasterKeys(opts)

mpub_b64 = open('/etc/salt/pki/master/master.pub', 'r').read()
print "== master.pub:==\n{0}\n".format(mpub_b64)

print "== make signature mpub_64 with master_sign.pem =="
mpub_sig_bin = salt.crypt.sign_message('/etc/salt/pki/master/master_sign.pem', mpub_b64)
mpub_sig_b64 = binascii.b2a_base64(mpub_sig_bin)
print mpub_sig_b64


mpub_vrfy_b64 = mpub_sig_b64
mpub_vrfy_bin = binascii.a2b_base64(mpub_vrfy_b64)

print "== verify signature ==\n{0}".format(mpub_vrfy_b64)

ret = salt.crypt.verify_signature('/etc/salt/pki/master/master_sign.pub', 
                                  mpub_b64,
                                  mpub_vrfy_bin)

if ret:
    print "yes"
else:
    print "no"
