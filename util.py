import os
import gzip
import json


def read_json(path: str):
    files = os.listdir(path)
    for file in files:
        if file.endswith('.json'):
            full_path = os.path.join(path, file)
            try:
                content = open(full_path).read()
                yield json.loads(content)
            except Exception as e:
                print("Error in func:", read_json.__name__, '\nError: ', e)

def read_json_zip(path: str):
    try:
        files = os.listdir(path)
        for file in files:
            full_path = os.path.join(path, file)
            content = gzip.open(full_path).read()
            yield json.loads(content)
    except Exception as e:
        print("Error in func:", read_json_zip.__name__, '\nError: ', e)

def read_json_zip_range(path: str, start: int, end: int):
    try:
        files = os.listdir(path)[start:end]
        for file in files:
            full_path = os.path.join(path, file)
            content = gzip.open(full_path).read()
            yield json.loads(content)
    except Exception as e:
        print("Error in func:", read_json_zip_range.__name__, '\nError: ', e)

def read_json_zip_files(file_list: list):
    for file in file_list:
        try:
            content = gzip.open(file).read()
            yield json.loads(content)
        except Exception as e:
            print("Error in func:", read_json_zip_files.__name__, '\nError: ', e)


if __name__ == "__main__":
    pass