#! /bin/bash 

scp -i /usr/local/cassandra/conf/cassandra.pem -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $4 root@$1:$2/$3/