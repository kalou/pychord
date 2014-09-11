import random
import json

allowed_classes = {}

def classname(cls):
    if hasattr(cls, '__class__'):
        _type = str(cls.__class__)
    else:
        _type = str(cls)
    if _type.startswith(cls.__module__):
        _type = _type.replace('%s.' % cls.__module__, '')
    return _type
    
def allow(cls):
    allowed_classes[classname(cls)] = cls
    return cls

class ProtocolMessage:
    _id = None
    _param = None

    def __init__(self, n=None, _id=None):
        self._param = n
        self._id = _id or random.randint(1, 2**32)

    def __repr__(self):
        return '%s(%s)' % (self.__class__, self._param)

    def encode(self):
        return json.dumps(self.as_dict())

    def as_dict(self):
        if hasattr(self._param, 'as_dict'):
            param = self._param.as_dict()
        else:
            param = self._param

        return {
                'type': classname(self),
                '_id': self._id,
                '_param': param
        }

    def rebuild(self, d):
        self._id = d['_id']
        if isinstance(d['_param'], dict) and  \
            d['_param'].get('type') in allowed_classes:
            self._param = allowed_classes[d['_param']['type']]()
            self._param.rebuild(d['_param'])
        else:
            self._param = d['_param']

    @classmethod
    def decode(self, data):
        d = json.loads(data)
        if d['type'] in allowed_classes:
            inst = allowed_classes[d['type']]()
            inst.rebuild(d)
            return inst

class ProtocolAnswer(ProtocolMessage):
    def __init__(self, orig_msg=None, n=None):
        if orig_msg:
            self._id = orig_msg._id
        self._param = n

@allow
class HelloMessage(ProtocolMessage):
    pass

@allow
class HelloAnswer(ProtocolAnswer):
    pass

@allow
class JoinMessage(ProtocolMessage):
    pass

@allow
class JoinAnswer(ProtocolAnswer):
    pass

@allow
class SuccessorMessage(ProtocolMessage):
    pass

@allow
class SuccessorAnswer(ProtocolAnswer):
    pass

@allow
class PredecessorMessage(ProtocolMessage):
    pass

@allow
class PredecessorAnswer(ProtocolAnswer):
    pass

@allow
class FindSuccessorMessage(ProtocolMessage):
    pass
    
@allow
class FindSuccessorAnswer(ProtocolAnswer):
    pass

@allow
class ClosestPrecedingMessage(ProtocolMessage):
    pass

@allow
class ClosestPrecedingAnswer(ProtocolAnswer):
    pass

@allow
class NotifySuccessorMessage(ProtocolMessage):
    pass

@allow
class NotifySuccessorAnswer(ProtocolAnswer):
    pass

@allow
class NotifyPredecessorMessage(ProtocolMessage):
    pass

@allow
class NotifyPredecessorAnswer(ProtocolAnswer):
    pass

@allow
class UpdateFingerTableMessage(ProtocolMessage):
    pass

@allow
class UpdateFingerTableAnswer(ProtocolAnswer):
    pass

@allow
class BroadcastMessage(ProtocolMessage):
    pass

@allow
class BroadcastAnswer(ProtocolAnswer):
    pass

@allow
class PutMessage(ProtocolMessage):
    pass

@allow
class PutAnswer(ProtocolAnswer):
    pass

@allow
class GetMessage(ProtocolMessage):
    pass

@allow
class GetAnswer(ProtocolAnswer):
    pass

@allow
class ProtocolNode:
    url = None
    identifier = None
    def __init__(self, node=None):
        if node:
            self.url = getattr(node, 'url', None)
            self.identifier = node.identifier

    def rebuild(self, d):
        self.url = d['url']
        self.identifier = d['identifier']

    def as_dict(self):
        return {
            'type': 'ProtocolNode',
            'url': self.url,
            'identifier': self.identifier
        }

    def __repr__(self):
        return 'pNode %s@%s' % (self.identifier, self.url,)

def encode(obj):
    return obj.encode()

def decode(data):
    return ProtocolMessage.decode(data)
