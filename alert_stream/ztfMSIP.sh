#!/bin/sh
#
# Creating a tarball for ztf msip alerts nightly. 
#
#---------------------------------
# programid0  |  commissioning   |
# programid1  |  public / MSIP   |
# programid2  |  collaboration   |
# programid3  |  Caltech*        |
# --------------------------------
# * private stream               |
# --------------------------------
#
# topic format: ztf_%Y%m%d_programid[0,1,2]
#
#topic_name=$1
#group_name=$2
#tarName=$3
topic_name=`printf 'ztf_%(%Y%m%d)T_programid1' -1`
tarName=`printf 'ztf_public_%(%Y%m%d)T' -1`
group_name=CatchPublicStream

export PATH="/epyc/opt/anaconda/bin:$PATH"
export PYTHONPATH="./alert_stream/python:$PYTHONPATH"

if [ ! -f ./${tarName}.tar.gz ] ; then
        # step:1
        # reading a topic and creating topic.tar file from all alerts.avro
        #
	echo -e "\n\n-------------------------------\n\n"
	echo \# topic name: $topic_name
	#
	python ./alert_stream/bin/catchMSIPStream.py ${topic_name} --group $group_name --tarName ${tarName}
        #
	# step:2
        # avro --> csv
        #
        if [ -f ./${tarName}.tar ] ; then
            echo -e "# time: $(date) \n"
            python ./alert_stream/bin/avro_csv.py ${tarName}
            echo -e "# time: $(date) \n"
        else
            echo "# tarball file does not exist."
        fi
        # step:3
        # tar --> tar.gz
        #    
        if [ -f ./${tarName}.tar ] ; then
            gzip ./${tarName}.tar
        fi

else
	echo topic: $topic_name has already been read.
fi
