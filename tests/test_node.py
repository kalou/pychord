from chord import node, protocol
import unittest
import sys

class TestNode(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.node1 = node.Node('inproc://localx1', None, 16)
        cls.node2 = node.Node('inproc://localx2', None, 16)

        cls.server1 = protocol.Server(cls.node1)
        cls.server1.start()

        cls.server2 = protocol.Server(cls.node2)
        cls.server2.start()

        cls.node2.join(cls.node1.url)

    @classmethod
    def tearDownClass(cls):
        cls.server1.stop()
        cls.server2.stop()

    def test_joined(self):
        self.assertEqual(self.node2.successor.identifier,
            self.node1.identifier)
        self.assertEqual(self.node2.predecessor.identifier,
            self.node1.identifier)
        self.assertEqual(self.node1.successor.identifier,
            self.node2.identifier)
        self.assertEqual(self.node1.predecessor.identifier,
            self.node2.identifier)

    def test_lookup(self):
        for x in xrange(100):
            v1 = self.node1.lookup(str(x)) 
            v2 = self.node2.lookup(str(x))
            print '%s = %s' % (v1, v2,)
            self.assertEqual(v1.url, v2.url)
