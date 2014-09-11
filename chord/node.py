from hash import ChordHash
import protocol
import random
import threading
import time

class Finger:
    start = None
    interval = None
    node = None

    def __repr__(self):
        return '.s=%s %s .n=%s' % (self.start, self.interval, self.node)

class Interval:
    def __init__(self, a, b):
        self.a = a
        self.b = b
        self.form("[]")

    def gt(self, x, y):
        return x > y

    def lt(self, x, y):
        return x < y

    def ge(self, x, y):
        return x >= y

    def le(self, x, y):
        return x <= y

    def form(self, s):
        self.f = s
        if len(s) != 2:
            raise Exception('Bad interval form %s' % s)

        self.c1 = { '[': self.ge, '(': self.gt }[s[0]]
        self.c2 = { ']': self.le, ')': self.lt }[s[1]]

        return self

    #def __len__(self):
    #    return (self.b - self.a) % self.m
            
    def __contains__(self, x):
        if self.a < self.b:
            return self.c1(x, self.a) and self.c2(x, self.b)
        elif self.a > self.b:
            return self.c1(x, self.a) or self.c2(x, self.b)
        else:
            if '[' in self.f or ']' in self.f:
                return True
            return False

    def __str__(self):
        return '%s%s, %s%s' % (self.f[0], self.a, self.b, self.f[1])
        

class FingerTable(dict):
    def __init__(self, n, m, node=None):
        self.n = n
        self.m = m
        for i in xrange(1, m+1):
            self[i] = Finger()
            self[i].start = (n + 2**(i-1)) % 2**m
            self[i].interval = Interval(self[i].start,
                (n + 2**i) % 2**m).form("[)")
            self[i].node = node

class EmptyApp:
    def on_broadcast(self, data):
        pass

    def on_new_node(self, node):
        pass

    def on_lost_node(self, node):
        pass

    def on_put(self, key, value):
        pass

    def on_get(self, key):
        pass

    def on_delete(self, key):
        pass

    def keys(self):
        return []

class Node(object):
    def __init__(self, url, app_inst=None, m=24):
        self.m = m
        self.identifier = self.set_url(url)
        self.finger = FingerTable(self.identifier, self.m, self)
        self.predecessor = self
        self.successor = self
        self.app_instance = app_inst or EmptyApp()

    @property
    def predecessor(self):
        return self._predecessor

    @predecessor.setter
    def predecessor(self, value):
        self._predecessor = value

    @property
    def successor(self):
        return self.finger[1].node

    @successor.setter
    def successor(self, value):
        self.finger[1].node = value
        if value != self:
            self.next_successor = protocol.Client(self).connect(value).successor

    def set_url(self, url):
        """Assign URL, and compute our identifier according to this"""
        self.url = url
        if isinstance(url, int):
            return url
        else:
            return ChordHash(url, self.m).value

    def lookup(self, key):
        return self.find_successor(ChordHash(key, self.m).value)

    def run_stabilization(self):
        """Periodical routine that detects dead nodes, and verifies
        successor list"""
        while True:
            self.check_successor()
            self.fix_fingers()
            self.move_keys()
            time.sleep(5)

    def lost(self, other):
        self.app_instance.on_lost_node(other)
        if other.identifier == self.successor.identifier:
            self.successor = self.next_successor
            protocol.Client(self).connect(self.successor).notify_predecessor(self)

        for i in range(1, self.m):
            if self.finger[i+1].node.identifier == other.identifier:
                self.finger[i+1].node = self.finger[i].node

        if self.predecessor.identifier == other.identifier:
            self.predecessor = self.find_predecessor(self.identifier)
        
    def check_successor(self):
        try:
            i = protocol.Client(self).connect(self.successor).predecessor
            if i.identifier in Interval(self.identifier, 
                self.successor.identifier).form('()'):
                self.successor = i
                protocol.Client(self).connect(self.successor).notify_predecessor(self)
            elif self.identifier in Interval(i.identifier,
                self.successor.identifier).form('()'):
                protocol.Client(self).connect(self.successor).notify_predecessor(self)
        except protocol.exception:
            try:
                protocol.Client(self).connect(self.next_successor).hello(self)
            except protocol.exception:
                print 'No backup yet, fixme/waiting'
                return
            if self.successor.identifier == self.predecessor.identifier:
                # special case for two-nodes network
                assert(self.next_successor.identifier == self.identifier)
                self.predecessor = self
            self.lost(self.successor)

    def fix_fingers(self):
        """Randomly check a finger"""
        i = random.randint(2, self.m)
        node = self.find_successor(self.finger[i].start)
        if node and node.identifier != self.finger[i].node.identifier:
            self.finger[i].node = node
        for i in range(1, self.m):
            ## Extend range for the known successor
            if self.finger[i+1].start in Interval(self.identifier,
                self.finger[i].node.identifier).form('[)'):
                    self.finger[i+1].node = self.finger[i].node

    def move_keys(self):
        """Send keys I have where my predecessor is a successor of the key"""
        if self.predecessor.identifier == self.identifier:
            # we did not rejoin, better skip this part
            print 'I am my own predecessor now'
            return
        for k in self.app_instance.keys():
            if ChordHash(k, self.m).value in Interval(
                self.identifier, self.predecessor.identifier).form('(]'):
                v = self.app_instance.on_get(k)
                print '%s xfering key %s to %s' % (self, ChordHash(k, self.m).value, self.predecessor,)
                protocol.Client(self).connect(self.predecessor).put(k, v)
                self.app_instance.on_delete(k)

    def join(self, url):
        """Join other node, and constructs our finger table with other help"""
        if self.successor != self:
            raise Exception('Already joined the network')

        try:
            other = protocol.Client(self).connect_url(url).joined(self)
            if other:
                self.init_finger_table(other)
            else:
                return False
        except protocol.exception:
            return False

        t = threading.Thread(None, self.run_stabilization)
        t.daemon = True
        t.start()
        return True

    def find_successor(self, id):
        """Find this id successor - using our finger table or fwding the
        question on the circle"""
        if self.successor == self:
            return self
        try:
            pred = self.find_predecessor(id)
            return protocol.Client(self).connect(pred).successor
        except protocol.exception, e:
            pass

    def find_predecessor(self, id):
        """Find the predecessor of an id. Used to refresh other nodes finger
        tables/successors"""
        if id == self.identifier:
            return self.predecessor
        pred = self
        while id not in Interval(pred.identifier, 
            protocol.Client(self).connect(pred).successor.identifier).form("(]"):
            pred = protocol.Client(self).connect(pred).closest_preceding(id)
        return pred

    def closest_preceding(self, id):
        """Find the closest node we know preceding id. Not including
        a node that handles this key, as we need his precedor"""
        for i in range(self.m, 0, -1):
            if self.finger[i].node.identifier in \
                Interval(self.identifier, id).form("()"):
                return self.finger[i].node
        return self

    def init_finger_table(self, other):
        print 'node said succ is %s' % other
        self.successor = other
        self.predecessor = protocol.Client(self).connect(self.successor).predecessor
        protocol.Client(self).connect(self.successor).notify_predecessor(self)
        protocol.Client(self).connect(self.predecessor).notify_successor(self)

        for i in range(1, self.m):
            ## Extend range for the known successor
            if self.finger[i+1].start in Interval(self.identifier,
                self.finger[i].node.identifier).form('[)'):
                    self.finger[i+1].node = self.finger[i].node
            else: ## Or find the next node
                self.finger[i+1].node = protocol.Client(self).connect(other).find_successor(
                    self.finger[i+1].start
                )

    def update_finger_table(self, other):
        """If this node is better than some in our finger tables, update"""
        touched = False
        for i in range(2, self.m):
            if other.identifier in Interval(self.identifier, self.finger[i].node.identifier).form('[)'):
                self.finger[i].node = other
                touched = True
        if touched:
            protocol.Client(self).connect(self.predecessor).update_finger_table(other)

    def update_others(self):
        """Notify nodes that we might be interesting for in terms of finger
        table"""
        for i in range(self.m, 0, -1):
            node = self.find_predecessor((self.identifier - 2**i) % 2**self.m)
            protocol.Client(self).connect(node).update_finger_table(self)

    def hello(self, other):
        return other.url

    ### Events and notifications
    def joined(self, other):
        successor = self.find_successor(other.identifier)
        if successor.identifier == other.identifier:
            ## Node already exists on network. Do not want.
            return None
        self.app_instance.on_new_node(other)
        return successor

    def put(self, key, value):
        return self.app_instance.on_put(key, value)

    def get(self, key):
        return self.app_instance.on_get(key)

    def delete(self, key):
        return self.app_instance.on_delete(key)

    def notify_successor(self, other):
        if other.identifier in Interval(self.identifier, self.successor.identifier):
            self.successor = other
            self.app_instance.on_new_node(other)
        else:
            try:
                protocol.Client(self).connect(self.successor).hello(self)
            except protocol.exception:
                self.successor = other

    def notify_predecessor(self, other):
        self.predecessor = other
        self.app_instance.on_new_node(other)

    def broadcast(self, stop_at, data):
        self.app_instance.on_broadcast(data)
        for i in range(1, self.m):
            # skip redundant finger
            if self.finger[i].node.identifier != \
             self.finger[i+1].node.identifier and self.finger[i].node != self:
                if self.finger[i].node.identifier in Interval(
                    self.identifier, stop_at).form('()'):
                    n = self.finger[i].node
                    if self.finger[i+1].node.identifier in Interval(
                        self.identifier, stop_at).form('()'):
                        new_stop_at = self.finger[i+1].node.identifier
                    else:
                        new_stop_at = stop_at
                    protocol.Client(self).connect(n).broadcast(new_stop_at, data)

        if self.finger[self.m].node.identifier in Interval(self.identifier,
            stop_at).form('()'):
            protocol.Client(self).connect(self.finger[self.m].node).broadcast(
                self.identifier, data
            )

        return True

    def __repr__(self):
        return 'Node %s@%s' % (self.identifier, self.url)

    def __cmp__(self, other):
        return self.identifier.__cmp__(other.identifier)
