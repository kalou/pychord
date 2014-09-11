from chord import node, protocol
import unittest
import sys
import random

class TestManyNodes(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.servers = []
        
        test_node = node.Node('inproc://local0', None, 6)
        server = protocol.Server(test_node)
        server.start()
        cls.servers.append(server)

        for x in xrange(1, 64):
            test_node = node.Node('inproc://local%s' % x, None, 6)
            server = protocol.Server(test_node)
            server.start()

            # randomly join the network
            print 'created node %s, joining' % test_node
            random_server = random.choice(cls.servers)
            if test_node.join(random_server.node.url):
                # then add to list of setup nodes
                cls.servers.append(server)
            else:
                print 'id conflict maybe'

    @classmethod
    def tearDownClass(cls):
        for srv in cls.servers:
            srv.stop()

    def test_network_size(self):
        self.assertGreater(len(self.servers), 20)

    def test_distribution(self):
        idmap = {}
        for x in xrange(1000):
            random_server = random.choice(self.servers)
            n = random_server.node.lookup(str(x))
            idmap[n.url] = idmap.get(n.url, 0) + 1

        self.assertEqual(len(idmap), len(self.servers))
        print idmap
