#!/usr/bin/python

from boto.s3.connection import S3Connection
import boto
connection = boto.s3.connection.S3Connection(aws_access_key_id='f45106ac6173430d9ef7e272b61c509a', aws_secret_access_key='468eb25bcb2c4d54b4cc9970bb705dac', port=8080, host='140.79.7.2', is_secure=False, calling_format=boto.s3.connection.OrdinaryCallingFormat())
bks=connection.get_all_buckets()
bk1=bks[0]
obs=bk1.get_all_keys()
print obs
index=int(raw_input("\nPlease input the index of the object: "))
ob=obs[index]
print ob
ob.get_contents_to_filename(ob.key)