from collections import defaultdict
import hashlib
import os
import sys


def chunk_reader(fobj, chunk_size=1024):
    while True:
        chunk = fobj.read(chunk_size)
        if not chunk:
            return
        yield chunk


def get_hash(filename, first_chunk_only=False, hash=hashlib.sha1):

    hashobj = hash()
    file_object = open(filename, 'rb')

    if first_chunk_only:
        hashobj.update(file_object.read(1024))
    else:
        for chunk in chunk_reader(file_object):
            hashobj.update(chunk)
    
    hashed = hashobj.digest()

    file_object.close()
    return hashed


def fill_size_hash_dict(path, hashes_by_size):
    
    for dirpath, dirnames, filenames in os.walk(path):

        for filename in filenames:
            full_path = os.path.join(dirpath, filename)
            try:
                full_path = os.path.realpath(full_path)
                file_size = os.path.getsize(full_path)
                hashes_by_size[file_size].append(full_path)
            except e:
                continue


def fill_hashes_for_1k(hashes_by_size, hashes_on_1k):

    for size_in_bytes, files in hashes_by_size.items():
        
        if len(files) < 2:
            continue    

        for filename in files:
            try:
                small_hash = get_hash(filename, first_chunk_only=True)
                hashes_on_1k[(small_hash, size_in_bytes)].append(filename)
            except e:
                continue

def check_for_duplicates(path, hash=hashlib.sha1):

    hashes_by_size = defaultdict(list)
    hashes_on_1k = defaultdict(list)  
    hashes_full = {}

    fill_size_hash_dict(path, hashes_by_size)

    fill_hashes_for_1k(hashes_by_size, hashes_on_1k)


    for __, files_list in hashes_on_1k.items():
        if len(files_list) < 2:
            continue  

        for filename in files_list:
            try: 
                full_hash = get_hash(filename, first_chunk_only=False)
                duplicate = hashes_full.get(full_hash)
                if duplicate:
                    print("damtxveva: {} da {}".format(filename, duplicate))
                    os.remove(filename)
                else:
                    hashes_full[full_hash] = filename
            except e:
                continue


if __name__ == "__main__":

    if sys.argv[1]:
        check_for_duplicates(sys.argv[1])
    else:
        print("არგუმენტი შეიყვანეთ")
