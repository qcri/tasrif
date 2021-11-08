"""Variable is a concept to share state across operators that are in the same ancestoral chain.
The class Variable provides a way to store a variabe that can be a user defined or a fundamental
python type. The design of the variable class follows the nullable object design pattern.
"""
#pylint: disable=R0903
class Variable:
    """Class to get and set a value implementing the nullable object design pattern.
    """

    def __init__(self, _value=None):
        self._value = _value

    def _get(self):
        return self._value

    def _set(self, _value):
        self._value = _value

    value = property(_get, _set)
