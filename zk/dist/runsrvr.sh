#!/bin/bash

#if [[ -z "$ZOOBINDIR" ]]
#then
#	echo "Error!! ZOOBINDIR is not set" 1>&2
#	exit 1
#fi
#
#. $ZOOBINDIR/zkEnv.sh

#TODO Include your ZooKeeper connection string here. Make sure there are no spaces.
# 	Replace with your server names and client ports.
export ZKSERVER=localhost:2181

# export ZKSERVER=tr-open-XX.cs.mcgill.ca:218XX,tr-open-XX.cs.mcgill.ca:218XX,tr-open-XX.cs.mcgill.ca:218XX
# export ZKSERVER=open-gpu-XX.cs.mcgill.ca:218XX,open-gpu-XX.cs.mcgill.ca:218XX,open-gpu-XX.cs.mcgill.ca:218XX

# java -cp $CLASSPATH:../task:.: DistProcess # linux

java -cp "../../lib/*;../task;." DistProcess "$@"


