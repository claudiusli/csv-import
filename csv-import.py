import os
import sys
import getopt
import getpass
import csv
import base64
import json
import requests

from pprint import pprint

# configuration values
config = dict(
    username = '',
    password = '',
    inputfile = '',
    #the number of rows to upload per bulk operation
    blocksize = 10000,
    append = False,
    dbname = '',
    authheader = ''
    )

def parse_args(argv):
    '''
    parse through the argument list and update the config dict as appropriate
    '''
    usage = 'python ' + os.path.basename(__file__) + ' -f <csv file to import> -u <username> [-b <# of records/update] [-d <dbname>] [-a]'
    try:
        opts, args = getopt.getopt(argv, "hf:b:d:au:", 
                                   ["file=",
                                    "blocksize=",
                                    "dbname=",
                                    "append",
                                    "uesrname="
                                    ])
    except getopt.GetoptError:
        print usage
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print usage
            sys.exit()
        elif opt in ("-f", "--file"):
            config['inputfile'] = arg
        elif opt in ("-b", "--blocksize"):
            config['blocksize'] = int(arg)
        elif opt in ("-d", "--dbname"):
            config['dbname'] = arg
        elif opt in ("-a", "--append"):
            config['append'] = True
        elif opt in ("-u", "--username"):
            config['username'] = arg

def init_config():
    if config['inputfile'] == '':
        print usage
        sys.exit()
    if config['username'] == '':
        print usage
        sys.exit()
    #let's check if the user specified a DB name
    #if not name it after the input file
    if config['dbname'] == '':
        config['dbname'] = config['inputfile'].split('.')[0]
    config['baseurl'] = 'https://{0}.cloudant.com/'.format(config['username'])
    config['dburl'] = config['baseurl'] + config['dbname']

def get_password():
    config['password'] = getpass.getpass('Password for {0}:'.format(config["username"]))
        
def authenticate():
    '''
    This essentially does:
    curl -X POST -i 'https://<username>.cloudant.com/_session' -H 'Content-type: application/x-www-form-urlencoded' -d 'name=<username>&password=<password>'
    '''
    header = {'Content-type': 'application/x-www-form-urlencoded'}
    url = config['baseurl'] + '_session'
    data = dict(name=config['username'],
                password=config['password'])
    response = requests.post(url, data = data, headers = header)
    if 'error' in response.json():
        if response.json()['error'] == 'forbidden':
            print response.json()['reason']
            sys.exit()
    config['authheader'] = {'Cookie': response.headers['set-cookie']}

def initialize_db():
    '''
    create the database
    '''
    r = requests.put(config['dburl'], headers = config['authheader'])
    if r.status_code == 412 and not config['append']:
        print 'The database "{0}" already exists. Use -a to add records'.format(config['dbname'])
        exit()

def updatedb(requestdata):
    '''
    posts <requestdata> to the database as a bulk operation
    <requestdata> is expected to be a json file which consists of multiple documents
    the form of <requestdata> is:
    {'docs': [{<doc1>}, {doc2}, ... {docn}]}
    '''
    headers = config['authheader']
    headers.update({'Content-type': 'application/json'})
    r = requests.post(
        config['dburl']+'/_bulk_docs',
        headers = headers,
        data = json.dumps(requestdata)
        )

def read_inputfile():
    '''
    read through the input file and do a bulk update for each <blocksize> rows
    '''
    with open(config['inputfile'], 'r') as fh:
        reader = csv.DictReader(fh)
        rowcounter = 0
        requestdata = dict(docs=[])
        for row in reader:
            if rowcounter >= config['blocksize']:
                #update db
                updatedb(requestdata)
                #reset the temp dict and counter
                requestdata = dict(docs=[])
                rowcounter = 0
            #add row to temp dict
            requestdata['docs'].append(row)
            #increment the row counter
            rowcounter += 1
        #write any remaining rows to the database
        updatedb(requestdata)

def main(argv):
    parse_args(argv)
    get_password()
    init_config()
    authenticate()
    initialize_db()
    read_inputfile()

if __name__ == "__main__":
    main(sys.argv[1:])
