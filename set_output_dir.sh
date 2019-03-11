#!/bin/bash

paths="postgresql-9.6.6/src/backend/optimizer/path/costsize.c 
BoundSketch/src/Driver.java 
BoundSketch/src/QueryGraph.java"

pwd="$PWD"

for path in $paths
do
    sed -i '' "s|info.txt|$pwd\/output\/info.txt|g" $path
    sed -i '' "s|log.txt|$pwd\/output\/log.txt|g" $path
    sed -i '' "s|output\/raw|$pwd\/output\/raw|g" $path
    sed -i '' "s|output\/results|$pwd\/output\/results|g" $path
done