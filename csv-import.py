import os
import sys
import getopt
import csv
import base64
import json
import requests

from pprint import pprint

# configuration values
config = dict(
    username=os.environ['cloudant_user'],
    credentials=os.environ['cloudant_credentials'],
    inputfile='',
    #the number of rows to upload per bulk operation
    blocksize = 10000,
    dbname = '',
    authheader = ''
    )

def parse_args(argv):
    '''
    parse through the argument list and update the config dict as appropriate
    '''
    try:
        opts, args = getopt.getopt(argv, "hf:b:d:", 
                                   ["file=",
                                    "blocksize=",
                                    "dbname="
                                    ])
    except getopt.GetoptError:
        print 'python',os.path.basename(__file__), '-f <csv file to import>'
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print 'python',os.path.basename(__file__), '-f <csv file to import>'
            sys.exit()
        elif opt in ("-f", "--file"):
            config['inputfile'] = arg
        elif opt in ("-b", "--blocksize"):
            config['blocksize'] = int(arg)
        elif opt in ("-d", "--dbname"):
            config['dbname'] = arg
    if config['inputfile'] == '':
        print 'python',os.path.basename(__file__), '-f <csv file to import>'
        sys.exit()
        
def initdbname():
    '''
    let's check if the user specified a DB name
    if not name it after the input file
    '''
    if config['dbname'] == '':
        config['dbname'] = config['inputfile'].split('.')[0]

def inithttp():
    config['authheader'] = {'Authorization': 'Basic '+config['credentials']}
    config['baseurl'] = 'http://{0}.cloudant.com/{1}'.format(
        config['username'],
        config['dbname']
        )
    config['posturl'] = config['baseurl']+'/_bulk_docs'
    config['postheader'] = {'Content-type': 'application/json'}

def initializedb():
    '''
    create the database
    '''
    #note this should eventually check to see if the DB is there first and
    #if so refuse to continue unless an override flag is supplied.
    initdbname()
    inithttp()
    r = requests.put(config['baseurl'], headers = config['authheader'])

def updatedb(requestdata):
    headers = config['authheader']
    headers.update(config['postheader'])
    r = requests.post(
        config['posturl'],
        headers = headers,
        data = json.dumps(requestdata)
        )
        
    #pprint(requestdata)

def read_inputfile():
    '''
    read through the input file and do a bulk update for each <blocksize> rows
    '''
    with open(config['inputfile'], 'r') as fh:
        reader = csv.DictReader(fh)
        rowcounter = 0
        requestdata = dict(docs=[])
        for row in reader:
            if rowcounter < config['blocksize']:
                #add row to temp dict
                requestdata['docs'].append(row)
                #increment the row counter
                rowcounter += 1
            else:
                #update db
                updatedb(requestdata)
                #reset the temp dict and counter
                requestdata = dict(docs=[])
                rowcounter = 0

def main(argv):
    parse_args(argv)
    initializedb()
    read_inputfile()

if __name__ == "__main__":
    main(sys.argv[1:])
