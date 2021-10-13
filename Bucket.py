#import module
import queue
import boto3
from threading import Thread
import functools



class Bucket:
    def __init__(self, bucket_name,profile="default"):
        """
        Constructs all needed atributes for S3 bucket and sets up threading count

        Args:
            bucket_name (string): the aws s3 bucket name
            profile (str, optional): aws profile credentials to use. Defaults to "default".
        """
        self._bucket_name  = bucket_name
        self._profile      = profile
        self._thread_count = 10
    
    def threaded(func):
        """
        creates threads that to executes function parameter and creates a unique client connection to s3 for each thread

        Args:
            func (function): this function will be executed by threads

        Returns:
            [function]: return result of the thread
        """
        
        @functools.wraps(func)
        def wrapper(self,*args, **kwargs):
            for i in range(self._thread_count):
                try:
                    session = boto3.Session(profile_name=self._profile)
                    client = session.client('s3')
                    client = boto3.session.Session().client('s3')
                    worker = Thread(target=func, args=(self,*args,client,))
                    worker.setDaemon(True)
                    worker.start()
                except Exception as e:
                    raise e

        return wrapper
     
    def s3_connection(func):
        """
        Decorator that establishes client and session to s3 and passes them to function

        Args:
            func (function): the function the wrapper will call passes the client and s3 session to the function its calling in args

        Returns:
            function: the warpper funciton
        """
        @functools.wraps(func)
        def wrapper(self,*args, **kwargs):
            try:
                session = boto3.Session(profile_name=self._profile)
                s3 = session.resource('s3')
                client = session.client('s3')
                client = boto3.session.Session().client('s3')
                return func(self,s3=s3,client=client,*args, **kwargs)
            except Exception as e:
                raise e

        return wrapper

        
    @s3_connection
    def get_bucket_objects(self,prefix="",*args, **kwargs):
        """
        method that gets a collection of objects in specified s3 bucket
        Args:
            prefix (str, optional): Only get files with prefix example "path/to/sub/folder" . Defaults to "" getting all objects.

        Returns:
            [<class 'boto3.resources.collection.s3.Bucket.objectsCollection'>]: object that contains s3 objects
        """
        try:

            s3 = kwargs.pop("s3")
            bucket = s3.Bucket(self._bucket_name)
            return bucket.objects.filter(Prefix=prefix)
        except Exception as e:
            raise e
    @s3_connection
    def get_all_bucket_versions(self,prefix="",*args, **kwargs):
        """
        method that gets a collection of objects in specified s3 bucket
        Args:
            prefix (str, optional): Only get files with prefix example "path/to/sub/folder/" . Defaults to "" getting all objects.

        Returns:
            [<class 'boto3.resources.collection.s3.Bucket.objectsCollection'>]: object that contains s3 objects
        """
        try:
            s3 = kwargs.pop("s3")
            bucket = s3.Bucket(self._bucket_name)
            return bucket.object_versions.filter(Prefix=prefix)
        except Exception as e:
            raise e

    def get_bucket_object_versions(self,object_name,client):
        """
        method that returns list of all versions for object with value object name
        Args:
            object_name (str): the s3 object full name
            [client]: allows s3 operations to be performed
        Returns:
            [versions]: list of all s3 versions of the object
            
        """
        try:
            versions = client.list_object_versions(Bucket = self._bucket_name, Prefix = object_name)
            return versions
        except Exception as e:
            raise e

    @threaded
    def delete_object_versions_helper(self,objects_queue,client):
        """Grab object from queue and delete it and deletes any delete markers

        Args:
            [client]: allows s3 operations to be performed
            objects_queue ([queue]): queue of s3 objects
        """
        while True:
            try:
                bucket_object = objects_queue.get()
                versions = self.get_bucket_object_versions(bucket_object.key,client)
                versions_list = versions.get('Versions')
                for version in versions_list:
                    #loop through versions and if latest dont delete
                    if not version.get("IsLatest"):
                        key = version.get('Key')
                        version_id = version.get('VersionId')                        
                        client.delete_object(Bucket = self._bucket_name, Key= key, VersionId = version_id)
                #remove delete markers for the object
                self.delete_object_delete_marker_helper(versions,client)
                objects_queue.task_done()
            except Exception as e:
                raise e

    
    def delete_object_delete_marker_helper(self,versions,client):
        """determines if versions of an object are delete markers and deletes them

        Args:
            versions ([type]): list of all s3 objects of the object to be deleted
            [client]: allows s3 operations to be performed
        """
        try:
            delete_markers_list = versions.get('DeleteMarkers')
            #only delete markers if any exists
            if delete_markers_list:
                for delete_marker in delete_markers_list:
                    #loop through delte markers for the object and if latest dont delete
                    key = delete_marker.get('Key')
                    version_id = delete_marker.get('VersionId')
                    client.delete_object(Bucket = self._bucket_name, Key= key, VersionId = version_id)
        except Exception as e:
            raise e
    
    def delete_bucket_versions(self,prefix=""):
        """Delete all versions, but does not delete lastest version of the object

        Args:
            prefix (str, optional): Only get files with prefix example "path/to/sub/folder" . Defaults to "" getting all objects.

        """
        try:
            bucket              = self.get_bucket_objects(prefix)
            objects_queue       = queue.Queue()
            self.delete_object_versions_helper(objects_queue)
            for obj in bucket:
                objects_queue.put(obj)
            
            objects_queue.join()
        except Exception as e:
            raise e
    
    @threaded
    def delete_all_bucket_objects_helper(self,objects_queue,client):
        """Grab object from queue and delete it

        Args:
            [client]: allows s3 operations to be performed
            objects_queue ([queue]): queue of s3 objects
        """
        while True:
            try:
                obj = objects_queue.get()
                key = obj.key
                version_id = obj.id
                client.delete_object(Bucket = self._bucket_name, Key = key, VersionId = version_id)
                objects_queue.task_done()
            except Exception as e:
                raise e
       
    def delete_all_objects(self,prefix=""):
        """Delete objects and its versions of objects, by default all of them or all object versions with prefix parameter

        Args:
            prefix (str, optional): Only get files with prefix example "path/to/sub/folder" . Defaults to "" getting all objects.

        """
        try:
            bucket              = self.get_all_bucket_versions(prefix)
            objects_queue       = queue.Queue()
            self.delete_all_bucket_objects_helper(objects_queue)
            for obj in bucket:
                objects_queue.put(obj)   
            objects_queue.join()
        except Exception as e:
            raise e
    
    def hello(self):
       print("hello")


