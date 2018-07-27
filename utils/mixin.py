
# see https://stackoverflow.com/a/6100595
def init_mixins(cls):
    """ Mixed-in class decorator. """
    classinit = cls.__dict__.get('__init__')  # Possibly None.

    # define an __init__ function for the class
    def __init__(self, *args, **kwargs):
        # call the __init__ functions of all the bases
        for base in [base for base in cls.__bases__ if base.__name__.endswith("Mixin")]:   # only initialize mixin classes
            base.__init__(self, *args, **kwargs)
        # also call any __init__ function that was in the class
        if classinit:
            classinit(self, *args, **kwargs)

    # make the local function the class's __init__
    setattr(cls, '__init__', __init__)
    return cls
