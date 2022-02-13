from asyncore import readwrite
from importlib.abc import Loader
import json

import os
import re
import yaml
import importlib

env_pattern = re.compile(r".*?\${(.*?)}.*?")
def env_constructor(loader, node):
    components = loader.construct_scalar(node).split()
    for group in env_pattern.findall(components[0]):
        if len(components) == 2:
            value = components[0].replace(f"${{{group}}}", os.environ.get(group, components[1]))
        else:
            value = components[0].replace(f"${{{group}}}", os.environ.get(group))
    return value

def get_loader():
    loader = yaml.SafeLoader
    loader.add_constructor("!ENV", env_constructor)
    return loader

def from_yaml(stream):
    """
    Runs parser for YAML files
    """
    yaml_file = yaml.load(stream, Loader=get_loader())
    context = load_modules(yaml_file['modules'])
    return parse(yaml_file['pipeline'], context)[0]


def from_json(stream):
    pipeline = json.loads(stream)
    return parse(pipeline['pipeline'])


def parse(obj, context):
    """
    Parses python object to create a tasrif pipeline object recursively
    
    Base Case: string, bool
    Recursive Case: list, dict
    """

    if isinstance(obj, dict):
        if len(list(obj.items())) == 1:
            key, value = list(obj.items())[0]
            parsed_value = parse(value, context)
            if key.startswith('$'):
                parsed = create_operator(key, parsed_value, context)
            else:
                parsed = {key: parsed_value}
        else:
            parsed = {}
            for key, value in obj.items():
                parsed_value = parse(value, context)
                if isinstance(key, str) and key.startswith('$'):
                    parsed[key] = create_operator(key, parsed_value, context)
                else:
                    parsed[key] = parsed_value
    elif isinstance(obj, list):
        parsed = []
        for value in obj:
            parsed.append(parse(value, context))
    elif isinstance(obj, str):
        if obj.strip().startswith('lambda'):
            parsed = eval(obj)
        elif obj.strip().startswith('$'):
            parsed = create_operator(obj, None, context)
        else:
            parsed = obj
    else:
        parsed = obj

    return parsed

def get_operator(spec, context):
    try:
        return context[spec]
    except KeyError as error:
        print(f"Error: Operator ${spec} is not defined. Make sure you are importing it in the modules section.")
        raise

def create_operator(key, value, context):
    key = key.replace('map', 'map_iterable')
    operator_specs = key[1:].split('.')
    operator = None

    # print(key, value)
    for i, operator_spec in enumerate(operator_specs):
        # print(i, operator_spec)
        if i == 0:
            if isinstance(value, list):
                if operator_spec == "sequence" or operator_spec == "compose":
                    operator = get_operator(operator_spec, context)(value)
                else:
                    args = []
                    kwargs = {}
                    for item in value:
                        if isinstance(item, tuple):
                            kwargs[item[0]] = item[1]
                        else:
                            args.append(item)
                    operator = get_operator(operator_spec, context)(*args, **kwargs)
            elif isinstance(value, dict):
                operator = get_operator(operator_spec, context)(**value)
            else:
                if value:
                    operator = get_operator(operator_spec, context)(value)
                else:
                    operator = get_operator(operator_spec, context)()

        else:
            operator = context[operator_spec](operator)

    return operator


def get_operator_name(spec):
    components = spec.split('_')
    name = ''.join(x.title() for x in components)
    if spec.endswith('dataset'):
        return name
    return name + 'Operator'


def load_modules(modules):
    context = {}
    for module in modules:
        for key, value in module.items():
            imported_module = importlib.import_module(key)
            for class_ in value:
                context[class_] = getattr(
                    imported_module, get_operator_name(class_))

    return context


if __name__ == "__main__":
    # with open("example.yaml", "r") as stream:
    with open("./examples/fitbit_intraday/yaml_config/sleep_dataset.yaml", "r") as stream:
        try:
            p = from_yaml(stream)
            df = p.process()
            print(df)
        except yaml.YAMLError as exc:
            print(exc)
