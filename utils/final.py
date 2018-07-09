from functools import wraps

# see this: https://stackoverflow.com/a/16056691
class Final(type):
    def __new__(cls, name, bases, classdict):
        for b in bases:
            if isinstance(b, Final):
                raise TypeError("type '{0}' is not an acceptable base type because it can not be subclassed (@final decorator used)".format(b.__name__))
        return type.__new__(cls, name, bases, dict(classdict))

# see this: https://stackoverflow.com/a/24956995
def final(cls):
    __name = str(cls.__name__)
    __bases = tuple(cls.__bases__)
    __dict = dict(cls.__dict__)
    
    for each_slot in __dict.get("__slots__", tuple()):
        __dict.pop(each_slot, None)
    
    __dict["__metaclass__"] = Final
    __dict["__wrapped__"] = cls
    
    return Final(__name, __bases, __dict)
