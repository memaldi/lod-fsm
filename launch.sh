export WNDIR=/usr/local/WordNet-3.0/dict

mvn exec:java -Dlaunch=$1 -Dargs.1=$2 -Dargs.2=$3 -Dargs.3=$4
