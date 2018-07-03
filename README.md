# ztf_public_alerts
creating a tarball for the entire ZTF public alert stream per night

You need the following packages/libraries in order to execute the code:

- avro
- fastavro
- confluent_kafka 

```
$ conda install -c defaults -c conda-forge librdkafka python-confluent-kafka python-avro cython fastavro
```

From the ztf_public_alerts directory:

```
$ cd ./alert_stream/
$ ./ztfMSIP.sh
```

Here are the steps being implemented in the `ztfMSIP.sh` code! 
- 1- Catching the entire alert stream (in `.avro` format) and dumpping them into a tarball
     ```
     $ python catchMSIPStream.py ${topic_name} --group ${group_name} --tarName ${tar_name}
     
     # topic_name: the bash script (ztfMSIP.sh) generates a topic name automatically in the following format
     # ztf_[current_date]_programid1
     # for example here is the topic name:  ztf_20180704_programid1 on July 4th 2018
     
     # group_name: a Kafka consumer group name
       for example: group_name=CatchPublicStream (the default in the bash script)
       
     # tar_name: the name of tarball
       the bash script genrates automatically a tarname based on the topic_name: ztf_[date].tar
       for example: ztf_20180704.tar
       
     Note: You can change all these parameters but you need to follow the instrcuction for the topic_name.
     ```
     
- 2- Extracting info from alerts packet and putting them in a `csv` file.
- 3- Compressing the tarball (`.tar --> .tar.gz`)
