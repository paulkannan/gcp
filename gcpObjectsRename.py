# Imports the Google Cloud client library
from google.cloud import storage

def rename_blobs(bucket_name):
    """Renames a group of blobs."""
    #storage client instance
    storage_client = storage.Client()

    #get bucket by name
    bucket = storage_client.get_bucket(bucket_name)

    #list files stored in bucket
    all_blobs = bucket.list_blobs()

    #Renaming all files to lowercase:
    for blob in all_blobs:
        new_name = str(blob.name).lower()
        new_blob = bucket.rename_blob(blob, new_name)
        print('Blob {} has been renamed to {}'.format( blob.name, new_blob.name))

rename_blobs("my-new-bucket")
