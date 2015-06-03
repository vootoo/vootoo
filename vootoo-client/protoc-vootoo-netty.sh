#!/bin/sh

orig_dir=$(pwd)
my_path_file=${0%}
my_path=${0%/*}
my_file=${0##*/}

MY_HOME=$orig_dir

if [[ $my_path_file != $my_file ]]
then
        cd $my_path
        MY_HOME=`pwd`
        cd $orig_dir
fi

if [[ $MY_HOME == '.' ]]
then
        MY_HOME=`pwd`
fi

cd $MY_HOME

#echo "protoc --java_out=src/main/java src/main/java/org/vootoo/client/netty/protocol/solr.proto"
protoc --java_out=src/main/java src/main/java/org/vootoo/client/netty/protocol/solr.proto

cd $orig_dir