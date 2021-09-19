import argparse,os
import boto3
from botocore.exceptions import NoCredentialsError

def walk_to_depth(root_path, depth):
        if depth < 0:
            for root, dirs, files in os.walk(root_path):
                yield [root, dirs[:], files]
            return

        elif depth == 0:
            return

        base_depth = root_path.rstrip(os.path.sep).count(os.path.sep)
        for root, dirs, files in os.walk(root_path):
            yield [root, dirs[:], files]
            current_depth = root.count(os.path.sep)
            if base_depth + depth <= current_depth:
                del dirs[:]

def upload_to_s3(local_file, s3_file, input_vars):

    s3 = boto3.client(service_name='s3',use_ssl=True, verify=False, endpoint_url=input_vars.aws_endpoint_url[0], aws_access_key_id=input_vars.aws_access_key[0],aws_secret_access_key=input_vars.aws_secret[0])
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


def process_files_f(base_dir,archive_dir,depth, input_vars):
    filedata=None
    print('processing directory {}'.format(base_dir)) 
    
    for (dirpath, dirnames, filenames) in walk_to_depth(base_dir,depth):
        for f in filenames:
            full_file_path = os.path.join(dirpath,f)
            print('processing file {}'.format(full_file_path))
            with open(full_file_path, 'r') as file:
                filedata = file.read()

            bucket_file_path = input_vars.bucket_path[0]+f;
            print('uploading file {} to {}'.format(full_file_path,bucket_file_path))
            uploaded = upload_to_s3(full_file_path, bucket_file_path, input_vars)

            #archive after upload
            with open(os.path.join(archive_dir,f), 'w') as file:
                file.write(filedata)

            print('deleting file {}'.format(full_file_path))
            #clear up the original file
            os.remove(full_file_path)
###
# python s3backup.py -d /Users/amitthk/projects/github/pytility/in -a 
# /Users/amitthk/projects/github/pytility/out 
# -k <accesskey> -s <accesskeysecret> 
# -u https://s3.ap-southeast-1.amazonaws.com 
# -b jvcdp-repo -n 1
# ###

def main():
    PATH = os.path.dirname(os.path.abspath(__file__))

    parser = argparse.ArgumentParser(description='Process files')
    parser.add_argument('-d','--sub_dir',nargs=1)
    parser.add_argument('-a','--archive_dir',nargs=1)
    parser.add_argument('-k','--aws_access_key', nargs=1)
    parser.add_argument('-s','--aws_secret', nargs=1)
    parser.add_argument('-u','--aws_endpoint_url', nargs=1)
    parser.add_argument('-b','--bucket_name', nargs=1)
    parser.add_argument('-p','--bucket_path', nargs=1)
    parser.add_argument('-n','--depth', nargs=1)

    input_vars = parser.parse_args()

    if any([input_vars.sub_dir is None, input_vars.archive_dir is None, 
        input_vars.aws_access_key is None, input_vars.aws_secret is None, 
        input_vars.aws_endpoint_url is None, input_vars.bucket_name is None,
        input_vars.bucket_path is None]):
        parser.print_usage()
        quit()
    base_dir = os.path.join(PATH,str(input_vars.sub_dir[0]))
    archive_dir = os.path.join(PATH,str(input_vars.archive_dir[0]))
    depth = 0
    if input_vars.depth is not None:
        depth = int(input_vars.depth[0])
    process_files_f(base_dir,archive_dir, depth , input_vars)

if __name__=="__main__":
    main()