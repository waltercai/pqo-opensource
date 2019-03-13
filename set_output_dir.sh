#!/bin/bash

output_paths="postgresql-9.6.6/src/backend/optimizer/path/costsize.c 
BoundSketch/src/Driver.java 
BoundSketch/src/QueryGraph.java"

sketch_paths="BoundSketch/src/OneDimensionalSketchUnc.java
BoundSketch/src/OneDimensionalSketchCon.java
BoundSketch/src/TwoDimensionalSketchUnc.java
BoundSketch/src/TwoDimensionalSketchCon.java
BoundSketch/src/ZeroDimensionalSketchUnc.java
BoundSketch/src/ZeroDimensionalSketchCon.java"

pwd="$PWD"

for path in $output_paths
do
    sed -i '' "s|info.txt|$pwd\/output\/info.txt|g" $path
    sed -i '' "s|log.txt|$pwd\/output\/log.txt|g" $path
    sed -i '' "s|output\/raw|$pwd\/output\/raw|g" $path
    sed -i '' "s|output\/results|$pwd\/output\/results|g" $path
    sed -i '' "s|join-order-benchmark\/|$pwd\/join-order-benchmark\/|g" $path
done

for path in $sketch_paths
do
    sed -i '' "s|sketch_dir|$pwd|g" $path
done

mkdir output/raw
mkdir output/results
mkdir output/raw/imdb
mkdir output/results/imdb
