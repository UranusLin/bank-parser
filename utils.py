import json
import os


def get_all_paths(directory):
    all_paths = []

    for root, dirs, files in os.walk(directory):
        for file in files:
            all_paths.append(os.path.join(root, file))
        for dir in dirs:
            all_paths.append(os.path.join(root, dir))

    return all_paths


# read json file and return dict
def read_json(file_name):
    with open(file_name, 'r', encoding='utf-8') as f:
        return json.load(f)
