#!/bin/bash

#if [[ -z "$ZOOBINDIR" ]]
#then
#    echo "Error!! ZOOBINDIR is not set" 1>&2
#    exit 1
#fi
#
#. $ZOOBINDIR/zkEnv.sh

# TODO Include your ZooKeeper connection string here. Make sure there are no spaces.
# 	Replace with your server names and client ports.
export ZKSERVER=localhost:2181,localhost:2182,localhost:2183
# export ZKSERVER=tr-open-14.cs.mcgill.ca:21804,tr-open-16.cs.mcgill.ca:21804,tr-open-17.cs.mcgill.ca:21804
# export ZKSERVER=open-gpu-XX.cs.mcgill.ca:218XX,open-gpu-XX.cs.mcgill.ca:218XX,open-gpu-XX.cs.mcgill.ca:218XX

# java -cp $CLASSPATH:../task:.: DistClient "$@" # linux
java -cp "../../lib/*;../task;." DistServer "$@" # win