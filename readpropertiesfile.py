import sys

separator = "="
keys = {}

if len(sys.argv)<3:
    print('Usage readpropertiesfile.py <properties filename> <property to fetch>')
    sys.exit()
with open(sys.argv[1]) as f:
    for line in f:
        if separator in line:
            key,value = line.split(separator,1)
            keys[key.strip()]=value.strip()
print(keys[sys.argv[2]])
