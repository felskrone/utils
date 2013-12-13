#!/usr/bin/python

import M2Crypto
import base64
import sys

class PrivPubTest(object):

    priv_key = None
    pub_key = None
    name = None

    def gen_key(self, key_length):
        print "Create {0} bit pair for {1}...".format(key_length, self.name)
        self.rsa_pair = M2Crypto.RSA.gen_key (key_length, 65537)
        self.rsa_pair.save_key ('{0}-private.pem'.format(self.name), None)
        self.rsa_pair.save_pub_key ('{0}-public.pem'.format(self.name))

    def get_pub_key(self):
        return self.pub_key

    def load_keypair(self):
        self.pub_key = M2Crypto.RSA.load_pub_key ('{0}-public.pem'.format(self.name))
        self.priv_key = M2Crypto.RSA.load_key  ('{0}-private.pem'.format(self.name))

    def load_pub_key(self, name):
        pub_key = M2Crypto.RSA.load_pub_key  ('{0}-public.pem'.format(name))
        return pub_key
        

    def create_enc_msg(self, msg, recv):
        print ""
        print "{0} creates message for {1}:".format(self.name, recv)
        print "{0} loads {1}'s public_key".format(self.name, recv)
        # create encrypted message
        pub_key = self.load_pub_key(recv)
        enc_text = pub_key.public_encrypt(msg, M2Crypto.RSA.pkcs1_oaep_padding)

        print ""
        print "The base64-encoded message is:"
        print "encoded with", pub_key
        print enc_text.encode('base64')
        print ""
        return enc_text.encode('base64')


class Alice(PrivPubTest):
    def __init__(self, bits):
        self.name = 'Alice'
        self.gen_key(bits)
        self.load_keypair()



class Bob(PrivPubTest):
    def __init__(self, bits):
        self.name = 'Bob'
        self.gen_key(bits)

    def decrypt_msg(self, msg):
        try:
            bob_priv = M2Crypto.RSA.load_key  ('{0}-private.pem'.format(self.name))
#            print "Bobs message:"
#            print "decoding with", self.priv_key
#            print msg
            dec_text = bob_priv.private_decrypt(base64.b64decode(msg), M2Crypto.RSA.pkcs1_oaep_padding)
#            print dec_text
        except Exception as g:
            print g
            print "{0}'s private key failed to decrypt the message".format(self.name)

if __name__ == '__main__':

    bits = int(sys.argv[1])
    loops = sys.argv[2]

    print ""
    alice = Alice(bits)

    print ""
    bob = Bob(bits)

    # create encoded message for bob
    msg = alice.create_enc_msg("test fusel", 'Bob')
    print "keys:{0}  loops:{1}".format(bits, loops)
    for i in range(5000):
        print i,
        sys.stdout.flush()
        bob.decrypt_msg(msg)


