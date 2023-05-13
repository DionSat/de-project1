# de-project
There are two scripts that we use for this project. The producer.py to produce/send messages to our datapipeline kafta client confluent. Confluent hold the message/records and then consumer.py consumes the messages to read them and process them. Currently the consumer process the messages into a dataframe and then asserts the data. Once that is done the dataframe is appended to a database.

# To Run Script
There are two ways to run the script by making it executable or running it through python3 in a virtual environment. Running it through a virtual environment would be recommended

## Make script executable chmod +x
**Run:** ```./consumer {confluent configuration ini}```

### Start a virtual environment using source {uservenv}/bin/activate
**Run:** ```python3 consumer {confluent configuration ini}```

# Change connection string for SQLAlchemy
To get the consumer working. A postgres database must be setup.
**Follow these instructions on how to set it up:** https://docs.google.com/document/d/1c8i2P2X9Qhvc_I9bFmqRFLGZQW_dl5bMBcGmqdXAat8/edit
Then in the data_helper.py change the conn_string on the delete_db(), create_db(), insert_db(), db_rowcount(), and check_tables() to match your current credentials. The credentials are in the 
format: ```postgresql+psycopg2://user:password@host:5432/database```
Also change the psycopg2.connect() to match the credentials

# Usage
```python3 consumer [-d] or [-h] {confluent configuration ini}```\
```python3 producer {confluent configuration ini}```\
```-h```: Help for usage \
```-d```: delete tables before processing
