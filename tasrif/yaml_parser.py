"""
Package for parsing YAML specifications for tasrif pipelines

The YAML file defines 2 sections, modules and pipeline.

modules:
Packages that are used in the pipeline are defined.

For operators imported from Tasrif, simply separate each word
in the operator name with an underscore, and use lowercase.
You should also omit 'Operator' from the end.

e.g. ConvertToDatetimeOperator -> convert_to_datetime


pipeline:
All operators should be preceeded by $ to identify them as operators.
"""


import importlib
import os
import re

import yaml

env_pattern = re.compile(r".*?\${(.*?)}.*?")


def _env_constructor(loader, node):
    """
    Replaces environment variables in YAML file
    """
    value = loader.construct_scalar(node)
    for group in env_pattern.findall(value):
        value = value.replace(f"${{{group}}}", os.environ.get(group))
    return value


def _get_loader():
    """
    Configures YAML loader
    """
    loader = yaml.SafeLoader
    loader.add_implicit_resolver("!ENV", env_pattern, None)
    loader.add_constructor("!ENV", _env_constructor)
    return loader


def from_yaml(stream, pipeline_name="pipeline"):
    """
    Runs parser for YAML files

    Args:
        stream (TextIOWrapper):
            yaml file stream
        pipeline_name (str):
            key in yaml file where pipeline is defined

    Returns:
        Tasrif pipeline object
    """
    yaml_file = yaml.load(stream, Loader=_get_loader())
    context = load_modules(yaml_file["modules"])
    return parse(yaml_file[pipeline_name], context)


def _parse_dict(obj, context):
    """
    Helper function that handles parsing of dict types
    """
    if len(list(obj.items())) == 1:
        key, value = list(obj.items())[0]
        parsed_value = parse(value, context)
        if key.startswith("$"):
            parsed = create_operator(key, parsed_value, context)
        else:
            parsed = {key: parsed_value}
    else:
        parsed = {}
        for key, value in obj.items():
            parsed_value = parse(value, context)
            if isinstance(key, str) and key.startswith("$"):
                parsed[key] = create_operator(key, parsed_value, context)
            else:
                parsed[key] = parsed_value

    return parsed


def parse(obj, context):
    """
    Parses python object to create a tasrif pipeline object recursively

    Base Case: string, bool
    Recursive Case: list, dict

    Args:
        obj:
            python data to be parsed
        context (Dict):
            Python dictionary holding all imported modules

    Returns:
        Tasrif pipeline object
    """

    if isinstance(obj, dict):
        parsed = _parse_dict(obj, context)
    elif isinstance(obj, list):
        parsed = []
        for value in obj:
            parsed.append(parse(value, context))
    elif isinstance(obj, str):
        if obj.strip().startswith("lambda"):
            # pylint: disable=W0123
            parsed = eval(obj)
        elif obj.strip().startswith("$"):
            parsed = create_operator(obj, None, context)
        else:
            parsed = obj
    else:
        parsed = obj

    return parsed


def _get_operator(spec, context):
    """
    Gets operator from context
    """
    try:
        return context[spec]
    except KeyError:
        print(
            f"Error: Operator ${spec} is not defined. Make sure you are importing it in the modules section."
        )
        raise


def create_operator(key, value, context):
    """
    Create operator instance along with its arguments

    Args:
        key:
            operator name
        value:
            operator arguments
        context (Dict):
            Python dictionary holding all imported modules

    Returns:
        Tasrif pipeline object
    """
    key = key.replace("map", "map_iterable")
    operator_spec = key[1:]
    operator = None

    if isinstance(value, list):
        if operator_spec in ("sequence", "compose"):
            operator = _get_operator(operator_spec, context)(value)
        else:
            args = []
            kwargs = {}
            for item in value:
                if isinstance(item, tuple):
                    kwargs[item[0]] = item[1]
                else:
                    args.append(item)
            operator = _get_operator(operator_spec, context)(*args, **kwargs)
    elif isinstance(value, dict):
        operator = _get_operator(operator_spec, context)(**value)
    else:
        if value:
            operator = _get_operator(operator_spec, context)(value)
        else:
            operator = _get_operator(operator_spec, context)()

    return operator


def _get_operator_name(spec):
    """
    Gives operator name from YAML short hand naming format
    """
    components = spec.split("_")
    name = "".join(x[:1].upper() + x[1:] for x in components)
    if spec.endswith("dataset"):
        return name
    return name + "Operator"


def load_modules(modules):
    """
    Import all functions listed in 'modules'

    Args:
        modules (List):
            List of modules to be imported

    Returns:
        Python dictionary holding all imported modules
    """
    context = {}
    for module in modules:
        for key, value in module.items():
            imported_module = importlib.import_module(key)
            for class_ in value:
                context[class_] = getattr(imported_module, _get_operator_name(class_))

    return context
