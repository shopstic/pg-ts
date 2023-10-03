# PostgreSQL introspection into TypeScript types

```bash
deno run -A --check ./src/cli.ts gen ...
```

```
USAGE EXAMPLE:

    gen --hostname=... --port=... --database=... --user=... --password=... --schemas="public" [--tableNaming="camel"] --outputDirectory=... --helperImportLocation=...

ARGUMENTS:

    --hostname             (string)                       
    --port                 (integer)                      
    --database             (string)                       
    --user                 (string)                       
    --password             (string)                       
    --schemas              (string | string...)           One or more postgres schemas to introspect
    --tableNaming="snake"  ("snake" | "camel" | "pascal") Naming convention for generated table names
    --outputDirectory      (string)                       
    --helperImportLocation (string)   
```