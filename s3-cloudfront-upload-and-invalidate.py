#!/usr/bin/python

import os
import sys
import time

import boto
import hashlib

# md5 function
def md5_for_file(filepath, block_size=2**20):
    f = open(filepath, 'rb')
 
    md5 = hashlib.md5()
    while True:
        data = f.read(block_size)
        if not data:
            break
        md5.update(data)
    return md5.hexdigest()

# Remote md5 function
def get_remote_md5sum(s3_file):
    # Download the file from the bucket
    s3_file.get_contents_as_string()

    return s3_file.md5
      
# You can only run three invalidations per distribution at a time
def number_of_running_invalidations():
    running_invalidations = 0

    invalidations = cloudfront_connection.get_invalidation_requests(distribution_id)

    for invalidation in invalidations:
        if invalidation.status == 'InProgress':
            running_invalidations += 1

    return running_invalidations

def validation_running(invalidation_id):
    invalidations = cloudfront_connection.get_invalidation_requests(distribution_id)

    for invalidation in invalidations:
        if invalidation.status == 'InProgress' and invalidation_id == invalidation.id: 
            return True
    return False

bucket_name = sys.argv[1]
distribution_id = sys.argv[2]
walk_dir = os.getcwd() 

# Connect to cloudfront
cloudfront_connection = boto.connect_cloudfront()

# Connect up to s3
s3_connection = boto.connect_s3()

# Set us to the right bucket
bucket = s3_connection.get_bucket(bucket_name)

# A list of files to invalidate
invalidate_files = []

# Walk all files
for root, subdirs, files in os.walk(walk_dir):
    for filename in files:
       disk_path = os.path.join(root, filename)
       s3_path = os.path.join(root.replace(os.getcwd(), ''), filename)

       # Check to see if a given file is in the bucket
       s3_file = boto.s3.key.Key(bucket, s3_path.lstrip('/'))

       # The file exists, now let's do the work to see if we need to invalidate it
       if bucket.get_key(s3_path):
          remote_md5 = get_remote_md5sum(s3_file)

          if md5_for_file(disk_path) != remote_md5:
              # Save the file for later invalidation
              invalidate_files.append(s3_path)
          else:
              # File matches, let's move on
              continue

       # Set some metadata
       s3_file.set_metadata('md5sum', md5_for_file(disk_path))

       # Upload
       s3_file.set_contents_from_filename(disk_path)

       # Let's make them publicly readable
       s3_file.set_acl('public-read')

       print 'Created and/or Uploaded a new version of %s' % s3_path

# Invalidate changed files

# Get us chunks of up to 1000 files
chunked_files = [invalidate_files[x:x+1000] for x in xrange(0, len(invalidate_files), 1000)]

# Record invalidations
invalidation_ids = []

for chunk in chunked_files:
    while True:
       invalidation_count = number_of_running_invalidations()

       # If we're running less than 3 invalidations on a distrbution
       if invalidation_count < 3:
           # Invalidate the files
           invalidation_request = cloudfront_connection.create_invalidation_request(distribution_id, chunk)
           invalidation_ids.append(invalidation_request.id)          
 
           print "Invalidating %s" % ",".join(chunk)
           break
       else:
           print "Waiting for the number of invalidations to drop from %s to 2" % invalidation_count
           time.sleep(15)

# Wait to exit until after all invalidations have completed
invalidations = cloudfront_connection.get_invalidation_requests(distribution_id)

for invalidation_id in invalidation_ids:
    while validation_running(invalidation_id):
        print "Waiting for invalidation %s to finish" % invalidation_id
        time.sleep(60)

    print "Invalidation %s complete" % invalidation_id
