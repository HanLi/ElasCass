#! /bin/bash

while true;
do
	sar -u 5 2 | grep Average | awk '{print 100-$8}'
done
