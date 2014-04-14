csv-import.py

requires:
requests

This script will read in a SCV file and import it into a Cloudant database. The top row of the header file is used to generate the field names. Each row is uploaded as a seperate record.
The script expects the user to have to environment variables defined.
cloudant_user: the Cloudant username for which the database will be created.
cloudant_credentials: the base64 encoded credentials of the user. You can generate this with 
python -c 'import base64; print base64.urlsafe_b64encode("<username>":"<password>")'

usage:
python csv-import.py -f <csv file to import> -b <# of records/update -d <dbname>

-f file= the name of the file to import
   The script expects as CSV file but the actual extension is irrelevant (see -d)

-b blocksize= the maximum number of records to import per _bulk_update operation.
   Default=1000. This does not need to be a factor of the total number of records. If there are not enough records remaining to fill a block it will just do the _bulk_update with whatever is left. In general this should be a large number since it reduces the overhead of http request but if blocksize*recordsize > 64Mb? there will be errors. Likewise if blocksize*recordsize > available memory you will probably see performance degredation from thrashing.

-d dbname= the name of the database to use
   Default=<file>(-.csv). The dbname can be any valid dbname. If none is supplied the filename is used as the dbname with any .csv stripped off.