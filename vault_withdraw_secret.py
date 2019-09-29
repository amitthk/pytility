import sys, os, json, time
import requests

PATH = os.path.dirname(os.path.abspath(__file__))
TOKEN_FILE = os.path.join(PATH,'ansible\\token')
VALUE_FILE = os.path.join(PATH,'ansible\\ansible_ssh_key')
CONST_RETRY_COUNT = 5

def write_to_file(file_name,f_output):
    with open(file_name,'w') as f:
        f.write(f_output)

def ParseBoolean (b):
    # ...
    if len(b) < 1:
        raise ValueError ('Cannot parse empty string into boolean.')
    b = b[0].lower()
    if b == 't' or b == 'true' or b == 'y' or b == 'yes' or b == '1':
        return True
    if b == 'f' or b == 'alse' or b == 'n' or b == 'no' or b == '0':
        return False
    raise ValueError ('Cannot parse string into boolean.')


def vault_login(request_url,payload,headers=None):
    if headers is None:
        headers = {'content-type':'application/json'}
    retry_count = CONST_RETRY_COUNT
    response = requests.post(request_url, data=json.dumps(payload),headers=headers)
    while retry_count >= 0:
        time.sleep(3) # wait 3 seconds then try again
        try:
            token = str(response.json()['auth']['client_token'])
            write_to_file(file_name=TOKEN_FILE,f_output=token)
            break
        except:
            retry_count-=1
            print ("File {} not ready, trying again. Retry count: {}".format(TOKEN_FILE, retry_count))

def vault_read_secret(request_url,field_name,should_decode=False,token=None):
    if token is not None:
        headers = {'content-type':'application/json', 'X-Vault-Token': token}
    retry_count = CONST_RETRY_COUNT
    response = requests.get(request_url,headers=headers)
    while retry_count >= 0:
        time.sleep(3) # wait 3 seconds then try again
        try:
            #print('response: '+str(response.json()))
            value = str(response.json()['data'][field_name])
            #print('Value: '+ value)
            if should_decode is True:
                value = value.decode('base64')
            write_to_file(file_name=VALUE_FILE,f_output=value)
            break
        except:
            retry_count-=1
            print ("File {} not ready, trying again. Retry count: {}".format(VALUE_FILE, retry_count))

def main():
    if len(sys.argv)<7:
        print('Usage vault_withdraw_secret.py <vault_addr> <ldap_id> <ldap_password> <key_name> <field_name> <base64decode? true/false>')
        sys.exit()
    vault_addr = sys.argv[1]
    ldap_id = sys.argv[2]
    ldap_pass = sys.argv[3]
    secret_key_name = sys.argv[4]
    secret_field_name = sys.argv[5]
    should_decode = ParseBoolean(sys.argv[6])
    payload = {'password':ldap_pass}
    full_url = '{}/v1/auth/ldap/login/{}'.format(vault_addr,ldap_id)
    vault_login(request_url=full_url,payload=payload, headers=None)
    read_secret_url = '{}/v1/secret/{}'.format(vault_addr,secret_key_name)
    with open(TOKEN_FILE,'r') as f:
        token = f.read()
        vault_read_secret(request_url=read_secret_url,field_name=secret_field_name,should_decode=should_decode, token=token)

if __name__=="__main__":
    main()
