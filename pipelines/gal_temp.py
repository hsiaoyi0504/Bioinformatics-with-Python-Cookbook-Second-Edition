import base64
from collections import defaultdict
import ftplib
import getpass
import warnings

from ruamel.yaml import YAML

from cryptography.fernet import Fernet
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

import pandas as pd

from bioblend.galaxy import GalaxyInstance
from bioblend.galaxy.histories import HistoryClient
from bioblend.galaxy.datasets import DatasetClient


warnings.filterwarnings('ignore')
# explain above, and warn


with open('galaxy.yaml.enc', 'rb') as f:
    enc_conf = f.read()
print(len(enc_conf))

#print('Please enter the password:')
#password = getpass.getpass()
password = b"bioinf"
with open('salt', 'rb') as f:
    salt = f.read()
kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=salt,
                 iterations=100000, backend=default_backend())
key = base64.urlsafe_b64encode(kdf.derive(password))
fernet = Fernet(key)

yaml = YAML()
conf = yaml.load(fernet.decrypt(enc_conf).decode())
print(conf)

server = conf['server']
ftp_server = server.split('/')[-1]
user = conf['user']
password = conf['password']
ftp_port = int(conf['ftp_port'])

history_name = 'bioinf_example'

gi = GalaxyInstance(url=server, key=api_key)
gi.verify = False
histories = gi.histories

for history in histories.get_histories():
    if history['name'] == history_name:
        histories.delete_history(history['id'])
    print(history['name'])

ds_history = histories.create_history(history_name)

ftp = ftplib.FTP() 
ftp.connect(host=ftp_server,port=ftp_port)
ftp.login(user=ftp_user, passwd=ftp_pass)
f = open('../raw/part-3L.vcf.gz','rb')
ftp.set_pasv(False)  # explain
ftp.storbinary('STOR part-3L.vcf.gz', f)
f.close() 
ftp.close()

gi.tools.upload_from_ftp('part-3L.vcf.gz', ds_history['id'])

contents = gi.histories.show_history(ds_history['id'], contents=True)
contents

def summarize_contents(contents):
    summary = defaultdict(list)
    for item in contents:
        summary['íd'].append(item['id'])
        summary['híd'].append(item['hid'])
        summary['name'].append(item['name'])
        summary['type'].append(item['type'])
        summary['extension'].append(item['extension'])
    return pd.DataFrame.from_dict(summary)

summarize_contents(contents)

vcf_ds = contents[0]
vcf_ds

gi.tools.get_tools()

vcf2tsv = gi.tools.get_tools(name='VCFtoTab-delimited:')[0]
gi.tools.show_tool(vcf2tsv['id'], io_details=True, link_details=True)

ds_history

def dataset_to_param(dataset):
    return dict(src='hda', id=dataset['id'])

tool_inputs = {'input': dataset_to_param(vcf_ds), 'g_option': True, 'null_filter': ''}
#hid!
print(tool_inputs)
gi.tools.run_tool(ds_history['id'], vcf2tsv['id'], tool_inputs=tool_inputs)
