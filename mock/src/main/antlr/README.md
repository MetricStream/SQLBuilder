The files in this directory are required for the generated ANTLR parser.

They were copied from the directory [plsql](https://github.com/antlr/grammars-v4/tree/master/sql/plsql) of the
ANTR community repo [grammars-v4](https://github.com/antlr/grammars-v4), which contains ANTLR grammar files for many
languages.

The copying happened on 2022-08-16 from commit bf9d634.

After copying, I had to add `TIMEZONE` to `non_reserved_keywords_pre12c` because we have code which uses `TIMEZONE` 
as a column name.
