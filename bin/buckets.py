#!/usr/bin/env python
import os
import argparse
import tempfile
from functools import total_ordering
from plato.shell.findutils import (Match, collect_size, find_files)


@total_ordering
class Bucket(object):
    
    def __init__(self, filename):
        self.filename = filename
        self.size = 0
        self.files = list()

    def __eq__(self, other):
        return self.size == other.size

    def __lt__(self, other):
        return self.size < other.size
    
    def put_file(self, filename, filesize):
        self.files.append(filename)
        self.size = self.size + filesize
        
    def get_path(self, folder):        
        return os.path.join(folder, self.filename)


def get_file_buckets(path, match, num_of_buckets, prefix='filebucket'):
    file_search = find_files(path, match, collect_size)
    # Create buckets.
    buckets = list()
    for bucket_num in range(num_of_buckets):
        bucket_filename = '%s_%d.lst' % (prefix, bucket_num)
        buckets.append(Bucket(bucket_filename))
    # Make sure files are sorted by size.
    sorted_files = sorted([(filename, filesize) for filename, filesize in file_search],
                          key=lambda pair: pair[1], reverse=True)
    # Round robins over buckets and try to maintain equal 
    # size in each of them.
    for filename, filesize in sorted_files:
        # Always put into the most empty bucket.
        emptiest_bucket = min(buckets)
        emptiest_bucket.put_file(filename, filesize)
    return buckets


def save_file_buckets(buckets, output_folder):
    for bucket in buckets:
        if not bucket:
            continue
        fhandle = open(bucket.get_path(output_folder), 'w+')
        for pathname in bucket.files:
            fhandle.write(pathname + '\n')
        fhandle.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser('File bucketing')
    parser.add_argument('datapath', help='Path to the data (usually files) '                        
                        'that would be distributed into multiple buckets')
    parser.add_argument('-outputdir', default=tempfile.mkdtemp(),
                        help='output directory where to put file lists')
    parser.add_argument('-bucketnum', help='number of buckets', default=10)
    args = parser.parse_args()
    
    match = Match(filetype='f')
    buckets = get_file_buckets(args.datapath, match, args.bucketnum)
    
    print([bucket.size for bucket in buckets])
    print([len(bucket.files) for bucket in buckets])
    
    save_file_buckets(buckets, args.outputdir)
    print(args.outputdir)
    
