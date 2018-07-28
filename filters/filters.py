import logging
logger = logging.getLogger(__name__)


# global instance of our filter definitions
filters = None

# base class for filter classes
class Base(object):
    def __init__(self, logger):
        self.logger = logger
    
    def covert_msg_incoming(self, msg, con):
        pass
    def covert_msg_outgoing(self, msg, con):
        pass
    def msg_incoming(self, msg, con):
        pass
    def msg_outgoing(self, msg, con):
        pass

# update filter class attributes
def update_attributes(attributes):
    global filters
    if filters:
        for key, value in attributes.items():
            setattr(filters, key, value)

# load and instantiate filter class
def load(code, attributes):
    global filters
    loc = {"Base": Base}
    try:
        filters = None
        exec(code, globals(), loc)
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as e:
        return "Error loading filter definitions: %s" % str(e)
    if "Filters" in loc:
        f = loc["Filters"](logger)                  # let it use our logger
        for key, value in attributes.items():       # set all attributes
            setattr(filters, key, value)
        filters = f                                 # only update global variable after properly initializing the newly created filters instance
    else:
        return "Error loading filter definitions: No class definition for 'Filters' found!"


# proxy functions

def proxy(name, msg, con):
    global filters
    try:
        if filters and hasattr(filters, name):
            return getattr(filters, name)(msg, con)
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as e:
        logger.error("Error calling filter '%s': %s" % (name, str(e)))

def covert_msg_incoming(msg, con):
    return proxy("covert_msg_incoming", msg, con)

def covert_msg_outgoing(msg, con):
    return proxy("covert_msg_outgoing", msg, con)

def msg_incoming(msg, con):
    return proxy("msg_incoming", msg, con)

def msg_outgoing(msg, con):
    return proxy("msg_outgoing", msg, con)
