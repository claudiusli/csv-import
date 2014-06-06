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
    authheader = '',
    view = False,
    index = False
    )
usage = 'python ' + os.path.basename(__file__) + ' -f <csv file to import> -u <username> [-b <# of records/update] [-d <dbname>] [-a] [-v] [-i]'

def parse_args(argv):
    '''
    parse through the argument list and update the config dict as appropriate
    '''
    try:
        opts, args = getopt.getopt(argv, "hf:b:d:au:vi", 
                                   ["help",
                                    "file=",
                                    "blocksize=",
                                    "dbname=",
                                    "append",
                                    "username=",
                                    "view",
                                    "index"
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
        elif opt in ("-v", "--view"):
            config['view'] = True
        elif opt in ("-i", "--index"):
            config['index'] = True

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
    Authenticate to the cloudant using username and password
    Get a session cookie and save it

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

    this essentially does:
    curl -X PUT 'https://<username>.cloudant.com/<dbname>' -H 'Cookie: <authcookie>'
    '''
    r = requests.put(config['dburl'], headers = config['authheader'])
    if r.status_code == 412 and not config['append']:
        print 'The database "{0}" already exists. Use -a to add records'.format(config['dbname'])
        sys.exit()

def updatedb(requestdata):
    '''
    posts <requestdata> to the database as a bulk operation
    <requestdata> is expected to be a json file which consists of multiple documents
    the form of <requestdata> is:
    {'docs': [{<doc1>}, {doc2}, ... {docn}]}

    this essentially does:
    curl -X POST 'https://<username>.cloudant.com/<dbname>/_bulk_docs' -H 'Cookie: <authcookie>' -H 'Content-type: application/json' -d '<requestdata>'
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
        fieldnames = reader.fieldnames
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
    return(fieldnames)

def make_view(fieldname, activate = False):
    '''
    Create a secondary index and a "_count" reduce on the fieldname.
    if <activate> is set to False the document will not be created as an active
    design doc. To activate it remove the "INACTIVE" from the name

    this essentially does:
    curl -X POST 'https://<username>.cloudant.com/<dbname>/' -H 'Content-type: application/json' -H 'Cooke: <authcookie>' -d '{"_id":"_design/<fieldname>_view","views":{"<fieldname>":{"map":"function(doc){if(doc.<fieldname>){emit(doc.<fieldname>,null);}}","reduce":"_count"}}}'

    view this with:
    curl -X GET 'https://<username>.cloudant.com/<dbname>/_design/<fieldname>_view/_view/<fieldname>'
    to see non reduced results append ?reduce=false
    '''
    headers = config['authheader']
    headers.update({'Content-type': 'application/json'})
    #construct data body here
    if activate == True:
        idstring = "_design/"  + fieldname
    else:
        idstring = "INACTIVE_design/" + fieldname
    #clean this up and add the reduce back
    requestdata = {"_id":idstring  + "_view",
                   "views":{fieldname:
                            {"map":'function(doc){{if(doc.{0}){{emit(doc.{0},null);}}}}'.format(fieldname),
                             "reduce":"_count"}}}
    r = requests.post(
        config['dburl'],
        headers = headers,
        data = json.dumps(requestdata)
    )

def make_index(fieldname, activate = False):
    '''
    Create a search index on fieldname
    if <activate> is set to False the document will not be created as an active
    design doc. To activate it remove the "INACTIVE" from the name

    this essentially does:
    curl -X POST 'https://<username>.cloudant.com/<dbname>' -H 'Content-type: application/json' -H 'Cooke: <authcookie>' -d '{"_id":"_design/<fieldname>_index","indexes":{\"<fieldname>\":{"index":"function(doc){if(doc.<fieldname>){index('<fieldname>',doc.<fieldname>,{"store":true})}}"}}}'

    view this with:
    curl -X GET 'https://<username>.cloudant.com/<dbname>/_design/<fieldname>_index/_search/<fieldname>?q=<fieldname>:<searchterm>'
    '''
    headers = config['authheader']
    headers.update({'Content-type': 'application/json'})
    #construct data body here
    if activate == True:
        idstring = "_design/"  + fieldname
    else:
        idstring = "INACTIVE_design/" + fieldname
    #clean this up and add the reduce back
    requestdata = {"_id":idstring + "_index",
                   "indexes":{fieldname:
                              {"index":'function(doc){{if(doc.{0}){{index("{0}",doc.{0},{{"store":true}});}}}}'.format(fieldname)}}}
#"map":'function(doc){{if(doc.{0}){{emit(doc.{0},null);}}}}'.format(fieldname),
#                             "reduce":"_count"}}}
    r = requests.post(
        config['dburl'],
        headers = headers,
        data = json.dumps(requestdata)
    )


def make_catalog(fieldnames):
    '''
    take the <fieldnames> and create a bunch of seconary indices out of them
    '''
    #the index limit is hardcoded for now
    #DO NOT CHANGE THIS unless you really know what you're doing
    #YOU CAN CRUSH A CLUSTER if you're not careful
    cataloglimit = 5
    catalog = 0
    for fieldname in fieldnames:
        #make sure we don't activate more than 5 views/indexes
        catalog += 1
        if catalog <= cataloglimit:
            activate = True
        else:
            activate = False
        if config['view']:
            #make a secondary index
            make_view(fieldname, activate)
        if config['index']:
            #make a search index
            make_index(fieldname, activate)

def main(argv):
    parse_args(argv)
    init_config()
    get_password()
    authenticate()
    initialize_db()
    fieldnames = read_inputfile()
    make_catalog(fieldnames)

if __name__ == "__main__":
    main(sys.argv[1:])
