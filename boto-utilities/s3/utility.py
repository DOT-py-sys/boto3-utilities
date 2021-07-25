# author - dot.PY
# version - 0.1
# date - 24 July 2021
try:
    import logging
    import botocore
    import boto3
    import datetime
    import os
    import sys
    import threading
except Exception as ex:
    print(str(ex))

class ProgressPercentage(object):

    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify, assume this is hooked up to a single filename
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()

class utility(object):
    '''
    all utility example of s3
    Signal Reference :
        0 : Initialization
        1 : Reconnect
        2 : Error Connection
    '''
    def __init__(self,aws_access_key_id,
                 aws_secret_access_key,region_name):

        '''

        :param aws_access_key_id: If not given it will assume aws is already configured
        :param aws_secret_access_key: If not given it will assume aws is already configured
        :param region_name: If not given it will assume aws is already configured
            If nothing is given , it is mandatory that we should have aws cli configured
            Happy Hunting !!!
        '''

        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name
        self.error = False
        self.s3_client = None

    def s3_connect(self):

        '''
        s3 connect generally using aws config or profile ,
            to proceed furthur
        :return: None
        '''
        if not self.error:
            try:
                self.s3_client = boto3.client("s3",
                                            region_name='us-east-1',
                                            aws_access_key_id=self.aws_access_key_id,
                                            aws_secret_access_key=self.aws_secret_access_key)
            except Exception as ex:
                print('Could not create connection to aws')
                logging.error(ex)
                self.error = True

    def create_bucket(self,bucket_name):

        '''

        :param bucket_name: given is the bucket name .
        :return: None , success message
        '''
        if not self.error:
            try:
                location = {'LocationConstraint':self.region_name}
                self.s3_client.create_bucket(bucket_name,CreateBucketConfiguration=location)
            except Exception as ex:
                self.error = True
                print('Error creating bucket '+str(ex))

    def list_exitsting_buckets(self):
        '''

        :return: List All buckets
        '''
        bucket_list = self.s3_client.list_buckets()
        print('Total buckets - '+str(len(bucket_list['Buckets'])))
        for i in bucket_list['Buckets']:
            print('Bucket Name '+i['Name'])
            print('Creation Date '+datetime.datetime.strftime(i['CreationDate'],'%d-%b-%Y'))

    def upload_files(self,file_name,bucket_name,object_name = None):
        '''
        Upload file object to S3 bucket
        :param file_name:
        :param bucket_name:
        :return:
        '''
        try:
            if bucket_name in [i.name for i in self.s3_client.list_buckets()['Buckets'] if i]:
                if os.path.exists(file_name): #ignoring file size
                    if object_name is None:
                        object_name = file_name
                    resp = self.s3_client.upload_file(file_name,bucket_name,
                                                      object_name,
                                                      Callback=ProgressPercentage(file_name))
                    print(resp)
                else:
                    print('File Does Not Exists '+str(file_name))
            else:
                print('Invalid Bucket Name '+str(bucket_name))
        except Exception as ex:
            self.error = True
            print(ex)

    def download_files(self,bucket_name,object_name,write_back_file):

        '''

        :param bucket_name: The bucket from where you want to download files
        :param object_name:  the object name for the files
        :return: file
        '''

        try:
            with open(write_back_file,'wb') as f:
                self.s3_client.download_fileobj(bucket_name,object_name,write_back_file)
        except Exception as ex:
            print(str(ex))
            self.error = True

    def additional_config(self,thresold_in_gb,max_concurrency,num_download_attempts):

        '''

        :param thresold_in_gb: set up the mlti part thresold value
        :param max_concurrency: to reduce the downstream bandwidth increase concurrency
        :return:
        If thread use is disabled, transfer concurrency does not occur.
            Accordingly, the value of the max_concurrency attribute is ignored
        '''
        try:
            config = boto3.s3.transfer.TransferConfig(multipart_threshold = thresold_in_gb*1024,
                                                      max_concurrency = max_concurrency,
                                                      num_download_attempts = num_download_attempts,
                                                      use_threads = True)
            return config
        except Exception as ex:
            print(str(ex))

    def presigned_url(self,bucket_name,object_name,expiration=7200):

        """Generate a presigned URL to share an S3 object

        :param bucket_name: string
        :param object_name: string
        :param expiration: Time in seconds for the presigned URL to remain valid
        :return: Presigned URL as string. If error, returns None.
        """
        try:
            response = self.s3_client.generate_presigned_url('get_object',
                                                        Params={'Bucket': bucket_name,
                                                                'Key': object_name},
                                                        ExpiresIn=expiration)
        except Exception as e:
            print(str(e))
            self.error = True

    def create_presigned_url_expanded(self,client_method_name, method_parameters=None,
                                      expiration=3600, http_method=None):
        """Generate a presigned URL to invoke an S3.Client method

        Not all the client methods provided in the AWS Python SDK are supported.

        :param client_method_name: Name of the S3.Client method, e.g., 'list_buckets'
        :param method_parameters: Dictionary of parameters to send to the method
        :param expiration: Time in seconds for the presigned URL to remain valid
        :param http_method: HTTP method to use (GET, etc.)
        :return: Presigned URL as string. If error, returns None.
        """

        # Generate a presigned URL for the S3 client method
        s3_client = boto3.client('s3')
        try:
            response = s3_client.generate_presigned_url(ClientMethod=client_method_name,
                                                        Params=method_parameters,
                                                        ExpiresIn=expiration,
                                                        HttpMethod=http_method)
        except Exception as e:
            print(str(e))
            return None

        # The response contains the presigned URL
        return response

    def create_presigned_post(self,bucket_name, object_name,
                              fields=None, conditions=None, expiration=3600):
        """Generate a presigned URL S3 POST request to upload a file

        :param bucket_name: string
        :param object_name: string
        :param fields: Dictionary of prefilled form fields
        :param conditions: List of conditions to include in the policy
        :param expiration: Time in seconds for the presigned URL to remain valid
        :return: Dictionary with the following keys:
            url: URL to post to
            fields: Dictionary of form fields and values to submit with the POST
        :return: None if error.
        """

        # Generate a presigned S3 POST URL
        s3_client = boto3.client('s3')
        try:
            response = s3_client.generate_presigned_post(bucket_name,
                                                         object_name,
                                                         Fields=fields,
                                                         Conditions=conditions,
                                                         ExpiresIn=expiration)
        except Exception as e:
            print(str(e))
            return None

        # The response contains the presigned URL and required fields
        return response

