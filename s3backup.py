import argparse,os
import boto3
from botocore.exceptions import NoCredentialsError



def upload_to_s3(local_file, s3_file, input_vars):

    s3 = boto3.client(service_name='s3',endpoint_url=input_vars.aws_endpoint_url[0], aws_access_key_id=input_vars.aws_access_key[0],aws_secret_access_key=input_vars.aws_secret[0])
    try:
        s3.upload_file(local_file, input_vars.bucket_name[0], s3_file)
        print("Upload Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False


def process_files_f(base_dir,archive_dir,input_vars):
    filedata=None
    print('processing directory {}'.format(base_dir)) 
    
    for (dirpath, dirnames, filenames) in os.walk(base_dir):
        for f in filenames:
            full_file_path = os.path.join(dirpath,f)
            print('processing file {}'.format(full_file_path))
            with open(full_file_path, 'r') as file:
                filedata = file.read()

            with open(os.path.join(archive_dir,f), 'w') as file:
                file.write(filedata)
            
            uploaded = upload_to_s3(full_file_path, f, input_vars)

###
# python s3backup.py --sub_dir openshift --archive_dir archive\
#  --aws_access_key xyzx --aws_secret sdfdsafsa 
#  --aws_endpoint_url https://asdfsa
#  --bucket_name bucketname ###

def main():
    PATH = os.path.dirname(os.path.abspath(__file__))

    parser = argparse.ArgumentParser(description='Process files')
    parser.add_argument('-d','--sub_dir',nargs=1)
    parser.add_argument('-a','--archive_dir',nargs=1)
    parser.add_argument('-k','--aws_access_key', nargs=1)
    parser.add_argument('-s','--aws_secret', nargs=1)
    parser.add_argument('-u','--aws_endpoint_url', nargs=1)
    parser.add_argument('-b','--bucket_name', nargs=1)

    input_vars = parser.parse_args()

    if any([input_vars.sub_dir is None, input_vars.archive_dir is None, input_vars.aws_access_key is None, input_vars.aws_secret is None, input_vars.aws_endpoint_url is None, input_vars.bucket_name is None]):
        parser.print_usage()
        quit()
    base_dir = os.path.join(PATH,str(input_vars.sub_dir[0]))
    archive_dir = os.path.join(PATH,str(input_vars.archive_dir[0]))
    process_files_f(base_dir,archive_dir,input_vars)

if __name__=="__main__":
    main()