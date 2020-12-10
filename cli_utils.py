import json


def read_json_input(filename):
    with open(filename) as f:
        input_data = json.load(f)
        return input_data
