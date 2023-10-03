import { PgTypes } from "./types.ts";
import {
  captureExec,
  CliProgram,
  createCliAction,
  ExitCode,
  inheritExec,
  pathJoin,
  PgClient,
  Static,
  Type,
} from "./deps.ts";
import { pgTypesToNodes } from "./codegen/codegen_common.ts";
import { introspectPgTables } from "./introspect/introspect_table.ts";
import { introspectPgCompositeTypes } from "./introspect/introspect_composite.ts";
import { introspectPgDomains } from "./introspect/introspect_domain.ts";
import { introspectPgEnums } from "./introspect/introspect_enum.ts";
import { generateKyselyParsers, generateKyselySchemas, generateKyselySerializers } from "./codegen/codegen_kysely.ts";

export const NonEmptyString = (props?: Parameters<typeof Type.String>[0]) => Type.String({ minLength: 1, ...props });

const CodegenCliArgs = Type.Object({
  hostname: NonEmptyString(),
  port: Type.Integer({ minimum: 1, maximum: 65535 }),
  database: NonEmptyString(),
  user: NonEmptyString(),
  password: NonEmptyString(),
  schemas: Type.Union([NonEmptyString(), Type.Array(NonEmptyString())], {
    description: "One or more postgres schemas to introspect",
    examples: ["public"],
  }),
  tableNaming: Type.Optional(Type.Union([
    Type.Literal("snake"),
    Type.Literal("camel"),
    Type.Literal("pascal"),
  ], {
    description: "Naming convention for generated table names",
    examples: ["camel"],
    default: "snake",
  })),
  outputDirectory: NonEmptyString(),
  helperImportLocation: NonEmptyString(),
});

type CodegenCliArgs = Static<typeof CodegenCliArgs>;

async function writeOutput(output: string, file: string) {
  const formatted = (await captureExec({
    cmd: ["deno", "fmt", "-"],
    stderr: {
      inherit: true,
    },
    stdin: {
      pipe: output,
    },
  })).out;

  await Deno.writeTextFile(file, formatted);
}

async function checkOutput(file: string) {
  try {
    await inheritExec({
      cmd: ["deno", "check", file],
    });
  } catch (e) {
    console.error(`Failed type checking generated code in file: ${file}`);
    throw e;
  }
}

async function generate(args: CodegenCliArgs) {
  const {
    hostname,
    port,
    database,
    user,
    password,
    outputDirectory,
    helperImportLocation,
    tableNaming = "snake",
  } = args;

  const schemas = Array.isArray(args.schemas) ? args.schemas : [args.schemas];

  const client = new PgClient({ hostname, port, database, user, password });

  await client.connect();

  try {
    const pgCompositeTypesPromise = introspectPgCompositeTypes(client, schemas);
    const pgTablesPromise = introspectPgTables(client, schemas);
    const pgDomainsPromise = introspectPgDomains(client, schemas);
    const pgEnumsPromise = introspectPgEnums(client, schemas);

    const pgCompositeTypes = await pgCompositeTypesPromise;
    const pgTables = await pgTablesPromise;
    const pgDomains = await pgDomainsPromise;
    const pgEnums = await pgEnumsPromise;

    const types: PgTypes = {
      composites: pgCompositeTypes,
      tables: pgTables,
      domains: pgDomains,
      enums: pgEnums,
    };

    const nodes = pgTypesToNodes(types);

    const typesOutputPath = pathJoin(outputDirectory, "schemas");
    const parsersOutputPath = pathJoin(outputDirectory, "parsers");
    const serializersOutputPath = pathJoin(outputDirectory, "serializers");

    await Deno.mkdir(typesOutputPath, { recursive: true });
    await Deno.mkdir(parsersOutputPath, { recursive: true });
    await Deno.mkdir(serializersOutputPath, { recursive: true });

    const generatedSchemas = generateKyselySchemas({
      nodes,
      schemaInterfaceName: "Schema",
      helperImportLocation,
      tableNaming,
    });
    const generatedParsers = generateKyselyParsers({
      nodes,
      helperImportLocation,
      schemasImportDirectory: "../schemas",
    });
    const generatedSerializers = generateKyselySerializers({
      nodes,
      helperImportLocation,
      schemasImportDirectory: "../schemas",
    });

    await Promise.all([
      ...Object.entries(generatedSchemas).map(async ([schema, lines]) => {
        lines.unshift("// deno-lint-ignore-file");
        const code = lines.join("\n");
        await writeOutput(code, `${typesOutputPath}/${schema}.ts`);
      }),
      ...Object.entries(generatedParsers).map(async ([schema, lines]) => {
        lines.unshift("// deno-lint-ignore-file");
        const code = lines.join("\n");
        await writeOutput(code, `${parsersOutputPath}/${schema}.ts`);
      }),
      ...Object.entries(generatedSerializers).map(async ([schema, lines]) => {
        lines.unshift("// deno-lint-ignore-file");
        const code = lines.join("\n");
        await writeOutput(code, `${serializersOutputPath}/${schema}.ts`);
      }),
    ]);

    await Promise.all(
      [
        ...Object.keys(generatedSchemas).map(async (schema) => {
          await checkOutput(`${typesOutputPath}/${schema}.ts`);
        }),
        ...Object.keys(generatedParsers).map(async (schema) => {
          await checkOutput(`${parsersOutputPath}/${schema}.ts`);
        }),
        ...Object.keys(generatedSerializers).map(async (schema) => {
          await checkOutput(`${serializersOutputPath}/${schema}.ts`);
        }),
      ],
    );
  } finally {
    await client.end();
  }
}

await new CliProgram()
  .addAction(
    "gen",
    createCliAction(
      CodegenCliArgs,
      async (args) => {
        await generate(args);

        return ExitCode.Zero;
      },
    ),
  )
  .run(Deno.args);
