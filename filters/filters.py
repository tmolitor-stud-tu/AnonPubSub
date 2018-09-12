import logging
logger = logging.getLogger(__name__)


# global instance of our filter definitions
filters = None

# base class for filter classes
class Base(object):
    def __init__(self, logger):
        self.logger = logger
    
    def gui_command_incoming(self, command):
        pass
    def gui_command_completed(self, command, error):
        pass
    def gui_event_outgoing(self, event):
        pass
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
    filters = None
    f = None
    try:
        exec(code, {}, loc)
        if "Filters" in loc:
            f = loc["Filters"](logging.getLogger("filters.loaded_filter"))     # let it use a special logger
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as e:
        return "Exception loading filter definitions: %s" % str(e)
    if not f:
        return "Error loading filter definitions: No class definition for 'Filters' found!"
    for key, value in attributes.items():       # set all attributes
        setattr(f, key, value)
    filters = f                                 # only update global variable after properly initializing the newly created filters instance


# proxy functions

def proxy(name, *args, **kwargs):
    global filters
    try:
        if filters and hasattr(filters, name):
            return getattr(filters, name)(*args, **kwargs)
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as e:
        logger.error("Error calling filter '%s': %s" % (name, str(e)))

def gui_command_incoming(command):
    return proxy("gui_command_incoming", command)

def gui_command_completed(command, error=False):
    return proxy("gui_command_completed", command, error)

def gui_event_outgoing(event):
    return proxy("gui_event_outgoing", event)

def covert_msg_incoming(msg, con):
    return proxy("covert_msg_incoming", msg, con)

def covert_msg_outgoing(msg, con):
    return proxy("covert_msg_outgoing", msg, con)

def msg_incoming(msg, con):
    return proxy("msg_incoming", msg, con)

def msg_outgoing(msg, con):
    return proxy("msg_outgoing", msg, con)
