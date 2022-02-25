#!/bin/bash
for ((i=1;i<=10;i++));
do
	echo "ROUND $i";
	make project2b > ./test/out-"$i".log;
done