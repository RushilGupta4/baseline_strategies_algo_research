import os
from concurrent.futures import ThreadPoolExecutor


def get_files_in_dir(dir):
    return os.listdir(dir)


def get_executor(workers=5):
    return ThreadPoolExecutor(max_workers=workers)
