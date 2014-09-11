import pickle
from Crypto.Cipher import Blowfish
import random
import exception

key = None

def _pad(data, size=8):
    return data + '\x00' * (size - (len(data) % size))

def _bitstr(x):
    s = ''
    while x:
        s += chr(x & 0xff)
        x = x >> 8
    return s

def encrypt(data):
    if not key:
        return data
    return Blowfish.new(key).encrypt(_pad(data))

def decrypt(data):
    if not key:
        return data
    if len(data) % 8:
        raise exception.SecurityError('Invalid packet format')
    return Blowfish.new(key).decrypt(data)
