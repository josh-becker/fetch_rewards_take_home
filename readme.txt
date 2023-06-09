Instructions:
Before running the ETL.py file, you must install all the required Python packages with pip: boto3, cryptography, json, psycopg2, and datetime.

pip3 install boto3
pip3 install cryptography
pip3 install json
pip3 install psycopg2
pip3 install datetime

Then, in the directory where the ETL.py program is stored, you can run the program with the following command:
python3 ETL.py

--------------------------------------------------------

Assumptions:

We assume that all of the SQS messages are generated the day that the program is run, and so we store today's date value in the Postgres database as the date of the login.

Since the app version column in the Postgres database has an integer data type, we assume that the minor version of the software (for example, version 3.2.0 vs. version 3.0) included in the SQS message is irrelevant, and only the major version will be stored in the database.

--------------------------------------------------------

Next steps / future development considerations:

This application would likely be deployed as a nightly process that runs as part of some sort of batch job, so that it could run with minimal manual intervention.

The device ID and IP address of each login is masked using encryption. A new private and public encryption key is generated each time the ETL program is run. In a production environment, the public and private key should be generated in a separate program. Because this ETL program only encrypts data and does not decrypt it, only the public key is necessary to run the program, so it should only have access to the public key. The private key should be stored according to your organization's key/password/secret management policy and should only be accessible to programs that need unmasked IP and device ID values. The masked values stored in the Postgres database can be used as is to identify duplicates.

For a production setting, we might consider tradeoffs of speed vs. memory usage. The current program collects all logins on the SQS queue before writing them to the Postgres database. This may use excessive memory when many logins from the queue need to be held in memory at the same time. So, it may prove worthwhile to collect a certain number of records from SQS, process that data, write to Postgres, and then loop through that cycle again until there are no records left on the queue.