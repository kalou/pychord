from Crypto.Hash import SHA256

class ChordHash:
    def __init__(self, value, m):
        sha = SHA256.new()
        sha.update(value)
        self.digest = sha.digest()
        self.bits = [ (ord(self.digest[i/8]) & (1<<(i&7))) >> (i&7)
            for i in xrange(0, 256, 256/m) ][:m]

        self.value = sum(self.bits[i]<<(m-i-1) for i in range(m))
