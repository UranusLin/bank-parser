import os


def get_all_paths(directory):
    all_paths = []

    for root, dirs, files in os.walk(directory):
        for file in files:
            all_paths.append(os.path.join(root, file))
        for dir in dirs:
            all_paths.append(os.path.join(root, dir))

    return all_paths