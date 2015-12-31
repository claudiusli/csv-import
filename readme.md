csv-import.py

requires:
requests

This script will read in a CSV file and import it into a Cloudant database. The top row of the header file is used to generate the field names. Each row is uploaded as a separate record.

usage:
```
python csv-import.py -f <csv file to import> -u <username> [-b <# of records/update>] [-d <dbname>] [-g <LAT_COLUMN_NAME,LON_COLUMN_NAME>] [-a] [-v] [-i]
```

-f file= the name of the file to import
   The script expects as CSV file but the actual extension is irrelevant (see -d)

-u username the name of your Cloudant account.

-b blocksize= the maximum number of records to import per _bulk_update operation.
   Default=1000. This does not need to be a factor of the total number of records. If there are not enough records remaining to fill a block it will just do the _bulk_update with whatever is left. In general this should be a large number since it reduces the overhead of http request but if blocksize*recordsize > 64Mb? there will be errors. Likewise if blocksize*recordsize > available memory you will probably see performance degradation from thrashing.

-d dbname= the name of the database to use
   Default=<file>(-.csv). The dbname can be any valid dbname. If none is supplied the filename is used as the dbname with any .csv stripped off.

-a append force append mode
   if the database already exists (either the default or a supplied name) the default behavior is to exit with an error message. If -a is specified new records will be added to the database except for records with an existing _id. NOTE this can result in duplicate records.

-v create views for the first 5 fields (sorted in alphabetical order)

-i create search indexes for the first 5 fields (sorted in alphabetical order)

-g create geojson for latitude and longitude fields.  -g LAT_COLUMN_NAME,LON_COLUMN_NAME

* https://<username>.cloudant.com/<dbname>/_design/geodd/_geo/?lat=<latitude>&lon=<longitude>&radius=<radius>

-h help print the help message.
