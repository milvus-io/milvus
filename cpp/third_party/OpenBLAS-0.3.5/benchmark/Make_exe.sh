#!/bin/bash

for f in *.goto *.acml *.mkl *.atlas
do
	if [ -f "$f" ]; then
		mv $f `echo $f|tr '.' '_'`.exe
	fi
done

