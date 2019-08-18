import argparse,os


def process_files_f(base_dir,input_vars):
    filedata=None
    print('processing directory {}'.format(base_dir)) 
    
    for (dirpath, dirnames, filenames) in os.walk(base_dir):
        for f in filenames:
            print('processing file {}'.format(os.path.join(dirpath,f)))
            with open(os.path.join(dirpath,f), 'r') as file:
                filedata = file.read()

            for opt in input_vars.variables:
                keyval = opt.split('=')
                print('replacing {} with {}'.format(keyval[0],keyval[1]))         
                filedata = filedata.replace(keyval[0], keyval[1])

            with open(os.path.join(dirpath,f), 'w') as file:
                file.write(filedata)

def main():
    PATH = os.path.dirname(os.path.abspath(__file__))

    parser = argparse.ArgumentParser(description='Process files')
    parser.add_argument('-d','--sub_dir',nargs=1)
    parser.add_argument('-v','--variables', nargs='+')

    input_vars = parser.parse_args()

    if any([input_vars.sub_dir is None, input_vars.variables is None or len(input_vars.variables) < 1]):
        parser.print_usage()
        quit()
    base_dir = os.path.join(PATH,str(input_vars.sub_dir[0]))
    process_files_f(base_dir,input_vars)

if __name__=="__main__":
    main()