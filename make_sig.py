#!/usr/bin/python
import salt.config
import salt.crypt
import salt.utils
import binascii
import os
import sys


mconf = '/etc/salt/master'
opts = None
mpub_f = 'master.pub'
mpub_sig_f = 'master_pubkey_signature'

if os.path.isfile(mconf):
    opts = salt.config.master_config('/etc/salt/master')

def make_master_pair():
    if not os.path.isfile('/etc/salt/pki/master/master.pub'):
        print "==> Generating master.* key-pair in {0}".format(opts['pki_dir'])
        salt.crypt.gen_keys(opts['pki_dir'], 'master', 4096)
    else:
        print "==> master.* keypair already exists, skipping"

def make_sign_pair():
    if not os.path.isfile('/etc/salt/pki/master/master_sign.pub'):
        print "==> Generating signing key-pair in {0}".format(opts['pki_dir'])
        salt.crypt.gen_keys(opts['pki_dir'], 'master_sign', 4096)
    else:
        print "==> signing keypair master_sign.* already exists, skipping"

def make_sig():
    with salt.utils.fopen(opts['pki_dir'] + '/' + mpub_f) as fp_:
        print "==> Reading {0}/{1}".format(opts['pki_dir'], mpub_f)
        mpub_b64 = fp_.read()

    print "==> Calculating signature for master.pub"
    mpub_sig_bin = salt.crypt.sign_message(opts['pki_dir'] + '/master_sign.pem', mpub_b64)
    mpub_sig_b64 = binascii.b2a_base64(mpub_sig_bin)

    if os.path.isfile(opts['pki_dir'] + '/' + mpub_sig_f):
        with salt.utils.fopen(mpub_sig_f, 'r') as fp_:
            cur_sig = fp_.read()
            fp_.close()
            if not cur_sig == mpub_sig_b64:
                with salt.utils.fopen(mpub_sig_f, 'w+') as fp_:
                    print "==> Updating signature in {0}/{1}".format(opts['pki_dir'], mpub_sig_f)
                    fp_.write(mpub_sig_b64)
            else:
                print "==> Signature in {0}/{1} is still valid".format(opts['pki_dir'], mpub_sig_f)
    else:
        with salt.utils.fopen(mpub_sig_f, 'w+') as fp_:
            print "==> Writing signature to {0}/{1}".format(opts['pki_dir'], mpub_sig_f)
            fp_.write(mpub_sig_b64)


if __name__ == '__main__':
    print ""
    make_master_pair()
    make_sign_pair()
    make_sig()
