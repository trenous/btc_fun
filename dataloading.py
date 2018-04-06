import os
from os.path import commonprefix, isfile
import ipdb
import gzip
import threading
from queue import Queue, Empty
import boto3
import ipdb


def fetch(filename_zip, bucket):
    filename = filename_zip.split('.')[0]
    if not isfile(filename):
        bucket.download_file(filename_zip, filename_zip)
        with gzip.open(filename_zip, 'rb') as zipped:
            with open(filename, 'wb') as unzipped:
                unzipped.write(zipped.read())
        os.remove(filename_zip)
    with open(filename, 'r') as data:
        data = data.read()
    return data


def key_to_timestamp(key):
    return int(key.split('/')[-1].split('.')[0])

def list_files(basedir, t_start, t_end):
    prefix = commonprefix([str(t_start), str(t_end)])
    prefix = '{}/{}'.format(basedir, prefix)
    s3 = boto3.resource('s3')
    buck = s3.Bucket('ripbtce-data')
    objs = buck.objects.all().filter(Prefix=prefix)
    keys = [o.key for o in objs if t_start <= key_to_timestamp(o.key) <= t_end]
    return keys

def data_generator(pair, channel, t_start, t_end):
    basedir = 'bitfinex/t{}/{}'.format(pair, channel)
    os.makedirs(basedir, exist_ok=True)
    queue = Queue()
    files = list_files(basedir, t_start, t_end)
    prod = Producer(queue, files, fetch)
    prod.start()
    while prod.is_alive() or not queue.empty():
        try:
            yield queue.get(timeout=5)
        except Empty:
            pass

class Producer(threading.Thread):

    def __init__(self, queue, files, fetch):
        super().__init__()
        self.queue = queue
        self.files = files
        self.fetch = fetch

    def run(self):
        s3 = boto3.resource('s3')
        bucket = s3.Bucket('ripbtce-data')
        for f in self.files:
            self.queue.put(self.fetch(f, bucket))
