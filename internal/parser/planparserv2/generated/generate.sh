#!/bin/sh

alias antlr4='java -Xmx500M -cp "../../../../tools/antlr/antlr-4.11.1-complete.jar:$CLASSPATH" org.antlr.v4.Tool'
antlr4 -Dlanguage=Go -no-listener -visitor -package planparserv2 *.g4
