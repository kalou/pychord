from message import *
import exception
import zmq

zmq_context = zmq.Context()

class Client:
    """Pickle/Unpickle nodes and messages over some network"""
    def __init__(self, node):
        self.node = node

    def connect_url(self, url):
        if url == self.node.url:
            return self.node
        self.socket = zmq_context.socket(zmq.REQ)
        self.socket.connect(url)
        return self

    def connect(self, other):
        return self.connect_url(other.url)

    def sendrecv(self, message, cls):
        """Send a message. Expect a cls answer. Check ids"""
        self.socket.send(message.encode())
        answer = cls.decode(self.socket.recv())
        assert(answer._id == message._id)
        return answer

    @property
    def successor(self):
        """Ask other for his successor"""
        answer = self.sendrecv(SuccessorMessage(), SuccessorAnswer)
        return answer._param

    @property
    def predecessor(self):
        """Ask other for his predecessor"""
        answer = self.sendrecv(PredecessorMessage(), PredecessorAnswer)
        return answer._param

    def find_successor(self, id):
        answer = self.sendrecv(FindSuccessorMessage(id), FindSuccessorAnswer)
        return answer._param

    def closest_preceding(self, id):
        answer = self.sendrecv(ClosestPrecedingMessage(id),
            ClosestPrecedingAnswer)
        return answer._param

    def joined(self, node):
        answer = self.sendrecv(JoinMessage(ProtocolNode(node)), JoinAnswer)
        return answer._param

    def notify_successor(self, node):
        self.sendrecv(NotifySuccessorMessage(ProtocolNode(node)),
                NotifySuccessorAnswer
        )
        # XXX ignore result now

    def notify_predecessor(self, node):
        self.sendrecv(NotifyPredecessorMessage(ProtocolNode(node)),
                NotifyPredecessorAnswer
        )
        # XXX ignore result now

    def update_finger_table(self, node):
        self.sendrecv(UpdateFingerTableMessage(ProtocolNode(node)),
            UpdateFingerTableAnswer
        )
        # XXX ignore result now

    def hello(self, node):
        answer = self.sendrecv(HelloMessage(ProtocolNode(node)), HelloAnswer)
        return answer._param.url

    def broadcast(self, stop_at, data):
        self.socket.send(BroadcastMessage((stop_at, data,)))

    def put(self, key, value):
        answer = self.sendrecv(PutMessage((key, value,)), PutAnswer)
        return answer._param

    def get(self, key):
        answer = self.sendrecv(self.other, GetMessage(key), GetAnswer)
        return answer._param

class Server:
    """Actual server loop is run from here. Pass a node to me"""
    def __init__(self, node):
        self.node = node
        self.socket = zmq_context.socket(zmq.REP)
        self.socket.bind(node.url)

    def send(self, msg):
        """Encode and send the reply"""
        self.socket.send(msg.encode())

    def _dispatch_message(self, node, msg):
        if isinstance(msg, JoinMessage):
            ret = node.joined(msg._param)
            if ret:
                self.send(JoinAnswer(msg, ProtocolNode(ret)))
            else:
                self.send(JoinAnswer(msg))
        elif isinstance(msg, SuccessorMessage):
            self.send(SuccessorAnswer(msg, 
                ProtocolNode(node.successor)))
        elif isinstance(msg, PredecessorMessage):
            self.send(PredecessorAnswer(msg,
                ProtocolNode(node.predecessor))
            )
        elif isinstance(msg, FindSuccessorMessage):
            ret = node.find_successor(msg._param)
            if ret:
                self.send(FindSuccessorAnswer(msg, ProtocolNode(ret)))
            else:
                self.send(FindSuccessorAnswer(msg))
        elif isinstance(msg, ClosestPrecedingMessage):
            ret = node.closest_preceding(msg._param)
            if ret:
                self.send(ClosestPrecedingAnswer(msg, ProtocolNode(ret)))
            else:
                self.send(ClosestPrecedingAnswer(msg))
        elif isinstance(msg, UpdateFingerTableMessage):
            ret = node.update_finger_table(msg._param)
            self.send(UpdateFingerTableAnswer(msg, ret))
        elif isinstance(msg, NotifySuccessorMessage):
            ret = node.notify_successor(msg._param)
            self.send(NotifySuccessorAnswer(msg, ret))
        elif isinstance(msg, NotifyPredecessorMessage):
            ret = node.notify_predecessor(msg._param)
            self.send(NotifyPredecessorAnswer(msg, ret))
        elif isinstance(msg, HelloMessage):
            self.send(HelloAnswer(msg, msg._param))
        elif isinstance(msg, BroadcastMessage):
            self.node.broadcast(*msg._param)
            self.send(BroadcastAnswer(msg))
        elif isinstance(msg, PutMessage):
            ret = self.node.put(*msg._param)
            self.send(PutAnswer(msg, ret))
        elif isinstance(msg, GetMessage):
            ret = self.node.get(msg._param)
            self.send(GetAnswer(msg, ret))
        else:
            print 'Received illicit %s' % msg
            self.send(HelloAnswer())
            sys.exit(1)

    def process_events(self):
        while self.running:
            if self.socket.poll(10):
                msg = self.socket.recv()
                self._dispatch_message(self.node, decode(msg))

    def serve_forever(self):
        self.running = True
        while self.process_events():
            pass

    def start(self):
        import threading
        self.thread = threading.Thread(None, self.serve_forever)
        self.thread.daemon = True
        return self.thread.start()

    def stop(self):
        self.running = False
        print 'stopping me'
