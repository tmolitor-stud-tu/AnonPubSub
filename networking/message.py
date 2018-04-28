import json
import logging
logger = logging.getLogger(__name__)


class Message(object):
    
    # no empty dict as default for data because this will create one single dict object used every time this constructor is called without data argument
    def __init__(self, msg_type=None, data=None):
        self.msg = {"type": None, "data": {}}		# default values (None, {})
        if isinstance(msg_type, Message):
            msg_type = bytes(msg_type)
        if isinstance(msg_type, bytearray) or isinstance(msg_type, bytes):
            self.msg = json.loads(msg_type.decode("UTF-8"))
        elif msg_type is not None:
            self.set_type(msg_type)
            if isinstance(data, dict):
                # deep copy everything to account for reference based bugs
                self.msg["data"] = dict(json.loads(json.dumps(data, separators=(',',':'))))
            else:
                self.msg["data"] = dict()
    
    # message type interface
    def set_type(self, msg_type):
        self.msg["type"] = msg_type
        return self
    def get_type(self):
        return self.msg["type"]
    
    # message data interface (dict usage)
    def __getitem__(self, key):
        return self.msg["data"][key]
    def __setitem__(self, key, value):
        self.msg["data"][key] = value
    def __delitem__(self, key):
        del self.msg["data"][key]
    
    # message copy interface
    def __deepcopy__(self):
        return Message(bytes(self))
    def __copy__(self):
        return self.__deepcopy__()
    
    # serialisation interface
    def __str__(self):
        return json.dumps(self.msg, separators=(',',':'))
    def __bytes__(self):
        return str(self).encode("UTF-8")
