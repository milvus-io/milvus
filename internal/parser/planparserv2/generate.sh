#!/usr/bin/env sh

antlr4() {
    java -Xmx500M -cp "../../../scripts/antlr-4.13.2-complete.jar:$CLASSPATH" org.antlr.v4.Tool "$@"
}
rm -fr generated
antlr4 -Dlanguage=Go -package planparserv2 -o generated -no-listener -visitor Plan.g4
