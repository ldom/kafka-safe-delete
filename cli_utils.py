import json


"""
{
    'uid': 17,
    'environment': 'RND',
    'topics_to_recreate': [
        'aaa',
        'bbb'
    ]
}
"""


def read_json_input(filename):
    with open(filename) as f:
        input_data = json.load(f)
        return input_data
