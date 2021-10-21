
import re
from inspect import getfullargspec
import functools

class Variable:

    def __init__(self, _value=None):
        self._value = _value

    def _get(self):
        return self._value

    def _set(self, _value):
        self._value = _value

    value = property(_get, _set)


def match_exp(arg):
    if type(arg) == str:
        regex = r"(^{[^{}]*}$)|(^{{[a-zA-Z_]+\w*}})$"
        return re.search(regex, arg)
    return None

def try_parse_number(arg):
    result = None
    try:
        result = Variable(float(arg))
    except:
        result = Variable(arg)
    return result

def _define_variables(args):
    result = {}
    for key, value in args.items():
        match_result = match_exp(value)
        if match_result:
            value_str = match_result.group()[1:-1]
            if value_str.startswith('{'):
                result[key] = value_str[1:-1]
            elif value_str and value_str != "None" :
                if value_str[0].isdigit() or value[0] == '.':
                    result[key] = try_parse_number(value_str)
                else:
                    result[key] = Variable(value_str)
            else:
                result[key] = Variable()
    return result

def enable_variables():
    def _enable_variables(f):

        @functools.wraps(f)
        def wrapped(*args, **kwargs):
            local_args = locals()
            argspec = getfullargspec(f)
            all_args = list(zip(argspec.args, local_args['args']))

            if 'kwargs' in local_args:
                all_args += list(local_args['kwargs'].items())

            if argspec.varargs:
                other_args = [(f"{argspec.varargs}_{i}", value) for i, value in enumerate(local_args['args'][len(argspec.args):])]
                all_args += other_args

            all_args_dict = {key: value for key, value in all_args}
            var_args = _define_variables(all_args_dict)

            modified_args = list(local_args['args'])
            modified_kwargs = {}

            if 'kwargs' in local_args:
                modified_kwargs = local_args['kwargs']

            for key, value in var_args.items():
                if key in argspec.args:
                    idx = argspec.args.index(key)
                    if idx >= 0 and idx < len(modified_args):
                        modified_args[idx] = value
                if 'kwargs' in local_args and key in local_args['kwargs']:
                    modified_kwargs[key] = value
            f(*modified_args, **modified_kwargs)
            local_args['args'][0]._vars = var_args

        return wrapped
    return _enable_variables
