# ztf_public_alerts
creating a tarball for the entire ZTF public alert stream each night

From the ztf_public_alerts directory:

```
$ cd ./alert_stream/
$ ./ztfMSIP.sh
```

Here are the steps getting implemented in the ztfMSIP.sh code! 
1- Catching the entire alert stream (in .avro format) and dumpping them into a tarball
2- Extracting info from alerts packet and putting them in a csv file.
3- Compressing the tarball (.tar --> .tar.gz)
