The files in this directory are required for the generated ANTLR parser.

They were copied from the directory [Java](https://github.com/antlr/grammars-v4/tree/master/sql/plsql/Java) of the 
ANTR community repo [grammars-v4](https://github.com/antlr/grammars-v4), which contains ANTLR grammar files for many 
languages.

The copying happened on 2022-08-16 from commit bf9d634.

After copying, the following modifications were performed:
1. Converted the files from Java to Kotlin (using IntelliJ)
2. Added `package com.metricstream.jdbc.antlr` to the top of each file
