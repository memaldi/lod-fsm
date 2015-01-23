#!/bin/sh
export WNDIR=/usr/local/WordNet-3.0/dict
export MAVEN_OPTS=-Xmx24576m

mvn exec:java -Dlaunch=$1 -Dargs.1=$2 -Dargs.2=$3 -Dargs.3=$4 -Dargs.4=$5 -Dargs.5=$6 -Dargs.6=$7
