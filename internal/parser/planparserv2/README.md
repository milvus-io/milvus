# Generate Parser with Antlr4

## Install Antlr4

Please follow [install antlr4](https://github.com/antlr/antlr4/blob/master/doc/go-target.md) to install the antlr tool.

The version of antlr tool: `4.9`.

## Code Generate

After you install the antlr4, you can generate the parser code in golang with:

```shell
export CLASSPATH=".:${PWD}/antlr-4.9-complete.jar:$CLASSPATH"
alias antlr4='java -Xmx500M -cp "${PWD}/antlr-4.9-complete.jar:$CLASSPATH" org.antlr.v4.Tool'
alias grun='java -Xmx500M -cp "${PWD}/antlr-4.9-complete.jar:$CLASSPATH" org.antlr.v4.gui.TestRig'
```

```shell
antlr4 -Dlanguage=Go -package planparserv2 -o generated -no-listener -visitor Plan.g4
```

All generated code will be under directory `generated`.
