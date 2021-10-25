# Bucket
Bucket is a Class that helps clean up s3 buckets by delete s3 objects. Class provides threaded way to empty s3 bucket remove versions, delete markers in s3 bucket 

## Usage

```python
import Bucket
#pass the bucket you want to work with and the aws configure profile for credentials,will use default if nothing passed in for profile
bucket = Bucket("s3_bucket_name","aws_creds_profile_name")

# Delete delete marker and prevoius versions, can pass prefix so only deltes objects with object path, nothing passed will delete all delete markers in s3 bucket
#this will delete all delte markers in path/to/files/ and any subfolders
bucket.remove_delete_markers("/path/to/files/")
#this will delete all delte markers in s3 bucket
bucket.remove_delete_markers()

# Delete all objects and prevoius versions, can pass prefix so only deltes objects with object path, nothing passed will delete all all objects in s3 bucket
#this will delete all all objects in path/to/files/ and any subfolders
bucket.delete_all_objects("/path/to/files/")
#this will  all objects all delte markers in s3 bucket
bucket.delete_all_objects()

# Delete all object versions and prevoius versions, can pass prefix so only deltes objects with object path, nothing passed will delete all all object versions  in s3 bucket
#this will delete all all object versions  in path/to/files/ and any subfolders
bucket.delete_bucket_versions("/path/to/files/")
#this will delete all object versions in s3 bucket
bucket.delete_bucket_versions()


```