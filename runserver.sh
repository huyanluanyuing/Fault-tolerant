#!/bin/bash

source settings.sh

export CLASSPATH=".:gen-java:lib/*"

KV_PORT=`shuf -i 10000-10999 -n 1`
echo Port number: $KV_PORT

$JAVA_HOME/bin/java StorageNode `hostname` $KV_PORT $ZKSTRING /$USER
