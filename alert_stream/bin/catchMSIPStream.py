#!/usr/bin/env python

"""
*** Consumes stream for storing all alerts as .tar file. ***

Note: exit the script at 7 AM

Note: consumers with the same group ID share a stream.
"""

from __future__ import print_function
import argparse
import sys
import io
import os
import fastavro
import avro.schema
import json
import time
import tarfile
from time import strftime
from datetime import datetime
from lsst.alert.stream import alertConsumer

stopTIME = 7

def msg_text(message):
    """Remove postage stamp cutouts from an alert message.
    """
    message_text = {k: message[k] for k in message
                    if k not in ['cutoutDifference', 'cutoutTemplate', 'cutoutScience']}
    return message_text


def _loadSingleAvsc(file_path, names):
    """Load a single avsc file.
    """
    with open(file_path) as file_text:
        json_data = json.load(file_text)
    schema = avro.schema.SchemaFromJSONData(json_data, names)
    return schema


def combineSchemas(schema_files):
    """Combine multiple nested schemas into a single schema.
    Parameters
    ----------
    schema_files : `list`
        List of files containing schemas.
        If nested, most internal schema must be first.
    Returns
    -------
    `dict`
        Avro schema
    """
    known_schemas = avro.schema.Names()

    for s in schema_files:
        schema = _loadSingleAvsc(s, known_schemas)
    
    return schema.to_json()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('topic', type=str,
                        help='Name of Kafka topic to listen to.')
    parser.add_argument('--group', type=str,
                        help='Globally unique name of the consumer group. '
                        'Consumers in the same group will share messages '
                        '(i.e., only one consumer will receive a message, '
                        'as in a queue). Default is value of $HOSTNAME.')
    parser.add_argument('--tarName', type=str,
                        help='Name of tar file.')
    avrogroup = parser.add_mutually_exclusive_group()
    avrogroup.add_argument('--decode', dest='avroFlag', action='store_true',
                           help='Decode from Avro format. (default)')
    avrogroup.add_argument('--decode-off', dest='avroFlag',
                           action='store_false',
                           help='Do not decode from Avro format.')
    parser.set_defaults(avroFlag=True)

    args = parser.parse_args()

    # Configure consumer connection to Kafka broker
    #conf = {'bootstrap.servers': 'kafka:9092',
    #        'default.topic.config': {'auto.offset.reset': 'smallest'}}
    conf = {'bootstrap.servers': 'epyc.astro.washington.edu:9092,epyc.astro.washington.edu:9093,epyc.astro.washington.edu:9094',
            'default.topic.config': {'auto.offset.reset': 'smallest'}}
    if args.group:
        conf['group.id'] = args.group
    else:
        conf['group.id'] = os.environ['HOSTNAME']

    # Configure Avro reader schema
    schema_files = ["/epyc/data/ztfDB/pro_msip/ztf-avro-alert/schema/candidate.avsc",
                    "/epyc/data/ztfDB/pro_msip/ztf-avro-alert/schema/cutout.avsc",
                    "/epyc/data/ztfDB/pro_msip/ztf-avro-alert/schema/prv_candidate.avsc",
                    "/epyc/data/ztfDB/pro_msip/ztf-avro-alert/schema/alert.avsc"]

    # Start consumer and collect alerts in a stream
    with alertConsumer.AlertConsumer(args.topic, schema_files, **conf) as streamReader:
    
        with tarfile.open("/epyc/data/ztf/alerts/public/"+args.tarName+".tar","a") as tar:
            while True:
            
                try:
                    msg = streamReader.poll(decode=args.avroFlag)
             
                    if msg is None:
                        
                        print('currenttime: ',int(strftime('%H')))
                        if (int(strftime('%H')) >= stopTIME):
                            print("break break break \n")

                            break
                        else:
                            print("continue continue continue \n")
                            continue     				
                    
                    else:
                        for record in msg:
    	    
                            #record0 = msg_text(record)
                            candidate_data = record.get('candidate')
                            fn = str(candidate_data['candid'])+".avro"
                            
                            with io.BytesIO() as avro_file:
                                record0 = [record]
                                fastavro.writer(avro_file,(combineSchemas(schema_files)),record0)
                                avro_file.seek(0)
                                tarinfo = tarfile.TarInfo(name=fn)
                                tarinfo.size = len(avro_file.getvalue())
                                tarinfo.mtime = time.time()
                                tarinfo.mode = 0o744
                                tarinfo.type = tarfile.REGTYPE
                                tarfile.uid = tarfile.gid = 0 
                                tarfile.unmae = tarfile.gname = "root"
                                tar.addfile(tarinfo,avro_file)
                                                                                       
                            #print( "%s \t %8.9f \t %8.5f \t %8.5f \n" % \
                            #    (record.get('objectId'),candidate_data['jd'],candidate_data['ra'],candidate_data['dec']) )
                                
                except alertConsumer.EopError as e:
                    # Write when reaching end of partition
                    sys.stderr.write(e.message)
                    #continue
                except IndexError:
                    sys.stderr.write('%% Data cannot be decoded\n')
                except UnicodeDecodeError:
                    sys.stderr.write('%% Unexpected data format received\n')
                except KeyboardInterrupt:
                    sys.stderr.write('%% Aborted by user\n')
                    break
            
            with open('/epyc/data/ztfDB/log/logMSIPStatus.txt','a') as lg:
                lg.write('# we reached the end of stream at %s \n'%(strftime("%b %d %Y %H:%M:%S")))

            sys.exit()


if __name__ == "__main__":
    main()
