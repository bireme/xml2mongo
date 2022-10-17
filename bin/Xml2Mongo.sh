#!/bin/bash

cd /home/oliveirmic/Projetos-dev/Export2Mongo || exit

export SBT_OPTS="-Xms12g -Xmx18g -XX:+UseG1GC" 

sbt "runMain e2m.Xml2Mongo $1 $2 $3 $4 $5 $6 $7 $8 $9 ${10} ${11} ${12} ${13}"
ret="$?"

cd - || exit

exit $ret

