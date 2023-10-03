import { assertExists, groupBy, mapEntries } from "../deps.ts";
import { toCamelCase, toPascalCase, toSnakeCase } from "../deps/case.ts";
import { PgTableColumnType, PgType, PgTypeNode } from "../types.ts";
import { buildUserDefinedMap } from "./codegen_common.ts";

export type KyselyTypeMapping = Record<string, Record<string, string>>;
export type KyselyParser = (value: unknown) => unknown;
export type KyselyParserMapping = Record<string, Record<string, string>>;

const defaultTypeMapping: KyselyTypeMapping = {
  "pg_catalog": {
    // String-equivalent Types
    "bpchar": "string",
    "char": "string",
    "cidr": "string",
    "float4": "string",
    "float8": "string",
    "inet": "string",
    "macaddr": "string",
    "name": "string",
    "numeric": "string",
    "oid": "string",
    "regclass": "string",
    "regconfig": "string",
    "regdictionary": "string",
    "regnamespace": "string",
    "regoper": "string",
    "regoperator": "string",
    "regproc": "string",
    "regprocedure": "string",
    "regrole": "string",
    "regtype": "string",
    "text": "string",
    "time": "string",
    "timetz": "string",
    "uuid": "string",
    "varchar": "string",

    // Numbers
    "int2": "number",
    "int4": "number",
    "int8": "bigint",
    "xid": "number",

    // Booleans
    "bool": "boolean",

    // Geometric Types
    "box": "PgBox",
    "circle": "PgCircle",
    "line": "PgLine",
    "lseg": "PgLineSegment",
    "path": "PgPath",
    "point": "PgPoint",
    "polygon": "PgPolygon",

    // Byte Data
    "bytea": "Uint8Array",

    // Dates and Times
    "date": "Date",
    "timestamp": "Date",
    "timestamptz": "Date",

    // JSON Data
    "json": "any",
    "jsonb": "any",

    // TID Type
    "tid": "PgTid",
  },
};

const defaultHelperTypes = Object.values(defaultTypeMapping.pg_catalog).filter((t) => t.startsWith("Pg"));

function toValidVariableName(str: string): string {
  return str.replace(/[^a-zA-Z0-9_]/g, "_");
}

function toFullName(type: { schema: string; name: string }): string {
  return `${type.schema}.${type.name}`;
}

function convertTableName(name: string, convention: "camel" | "snake" | "pascal") {
  if (convention === "camel") {
    return toCamelCase(name);
  }

  if (convention === "snake") {
    return toSnakeCase(name);
  }

  if (convention === "pascal") {
    return toPascalCase(name);
  }
}

export function generateKyselySchemas(
  {
    nodes,
    tableNaming,
    typeMapping = defaultTypeMapping,
    fallbackType = "unknown",
    schemaInterfaceName = "Database",
    helperImportLocation,
    helperPrefix = "$$",
  }: {
    nodes: PgTypeNode[];
    helperImportLocation: string;
    tableNaming: "camel" | "snake" | "pascal";
    typeMapping?: KyselyTypeMapping;
    fallbackType?: string;
    schemaInterfaceName?: string;
    helperPrefix?: string;
  },
): Record<string, string[]> {
  const userDefinedMap = buildUserDefinedMap(nodes);
  const nodesBySchema = groupBy(nodes, (node) => node.data.schema);
  const schemaToVariableMap = mapEntries(
    nodesBySchema,
    ([schema, _]) => [schema, helperPrefix + toValidVariableName(schema)],
  );

  function isUserDefined(type: PgType) {
    return userDefinedMap.get(type.schema)?.has(type.name);
  }

  const linesBySchema = mapEntries(nodesBySchema, ([schema, nodes]) => {
    assertExists(nodes);

    const lines: string[] = [
      `import type {${defaultHelperTypes.join(", ")}} from ${JSON.stringify(helperImportLocation)};`,
    ];

    const renderType = (type: PgType, arrayDimensions?: number, defaultValue?: string) => {
      let mapped: string;

      if (isUserDefined(type)) {
        mapped = (schema === type.schema) ? type.name : `${schemaToVariableMap[type.schema]}.${type.name}`;
      } else {
        mapped = (typeMapping[type.schema]?.[type.name] ?? fallbackType) + ` /* ${type.schema}.${type.name} */`;
      }

      mapped = arrayDimensions !== undefined ? `${mapped}` + "[]".repeat(arrayDimensions) : mapped;

      return defaultValue ? `${helperPrefix}HasDefault<${mapped}>` : mapped;
    };

    const otherSchemas = Object.keys(nodesBySchema).filter((s) => s !== schema);

    lines.push.apply(
      lines,
      otherSchemas.map((otherSchema) => {
        return `import type * as ${schemaToVariableMap[otherSchema]} from ${JSON.stringify(`./${otherSchema}.ts`)};`;
      }),
    );

    lines.push(
      `type ${helperPrefix}HasDefault<T> = {`,
      "  readonly __select__: T;",
      "  readonly __insert__: T | undefined;",
      "  readonly __update__: T | undefined;",
      "}",
    );

    for (const node of nodes) {
      if (node.kind === "composite") {
        lines.push("", `// Composite type ${toFullName(node.original)}`);
        lines.push(`export interface ${node.data.name} {`);
        for (const attr of node.data.attributes) {
          lines.push(`  ${attr.name}: ${renderType(attr.type, attr.arrayDimensions)};`);
        }
        lines.push(`}`);
      } else if (node.kind === "domain") {
        lines.push("", `// Domain ${toFullName(node.original)}`);
        lines.push(`export type ${node.data.name} = ${renderType(node.data.type)};`);
      } else if (node.kind === "enum") {
        lines.push("", `// Enum ${toFullName(node.original)}`);
        lines.push(`export enum ${node.data.name} {`);
        for (const value of node.data.values) {
          lines.push(`  ${value.name} = ${JSON.stringify(value.name)},`);
        }
        lines.push(`}`);
      } else if (node.kind === "table") {
        lines.push("", `// Table ${toFullName(node.original)}`);
        lines.push(`export interface ${node.data.name} {`);
        for (const column of node.data.columns) {
          lines.push(
            `  ${column.name}: ${renderType(column.type, column.arrayDimensions, column.defaultValue)}${
              column.isNullable ? " | null" : ""
            };`,
          );
        }
        lines.push(`}`);
      }
    }

    lines.push(`export interface ${schemaInterfaceName} {`);
    for (const node of nodes) {
      if (node.kind === "table") {
        lines.push(`${convertTableName(node.original.name, tableNaming)}: ${node.data.name}`);
      }
    }
    lines.push("}");

    lines.push(`export type Namespaced${schemaInterfaceName} = {`);
    lines.push(`  [K in keyof ${schemaInterfaceName} as \`${schema}.\${K}\`]: ${schemaInterfaceName}[K]`);
    lines.push(`}`);

    return [schema, lines];
  });

  return linesBySchema;
}

export function generateKyselyParsers(
  {
    nodes,
    parserMapping = {},
    helperImportLocation,
    schemasImportDirectory,
    helperPrefix = "$$",
  }: {
    nodes: PgTypeNode[];
    parserMapping?: KyselyParserMapping;
    helperImportLocation: string;
    schemasImportDirectory: string;
    helperPrefix?: string;
  },
): Record<string, string[]> {
  const nonTableNodes = nodes.filter((node) => node.kind !== "table");
  const userDefinedMap = buildUserDefinedMap(nonTableNodes);
  const nodesBySchema = groupBy(nonTableNodes, (node) => node.data.schema);
  const schemaToVariableMap = mapEntries(
    nodesBySchema,
    ([schema, _]) => [schema, helperPrefix + toValidVariableName(schema)],
  );

  function isUserDefined(type: PgType) {
    return userDefinedMap.get(type.schema)?.has(type.name);
  }

  return mapEntries(nodesBySchema, ([schema, nodes]) => {
    assertExists(nodes);

    const lines: string[] = [
      `import {${nodes.map((node) => node.data.name).join(",")}} from ${
        JSON.stringify(`${schemasImportDirectory.replace(/\/+$/, "")}/${schema}.ts`)
      };`,
      `import * as ${helperPrefix} from ${JSON.stringify(helperImportLocation)};`,
      `import type {${defaultHelperTypes.join(", ")}} from ${JSON.stringify(helperImportLocation)};`,
    ];

    const otherSchemas = Object.keys(nodesBySchema).filter((s) => s !== schema);

    lines.push.apply(
      lines,
      otherSchemas.map((otherSchema) => {
        return `import * as ${schemaToVariableMap[otherSchema]} from ${JSON.stringify(`./${otherSchema}.ts`)};`;
      }),
    );

    const renderValue = (type: PgType, value: string, arrayDimensions?: number) => {
      let fn: string | undefined;

      if (isUserDefined(type)) {
        fn = ((schema !== type.schema) ? `${schemaToVariableMap[type.schema]}.` : "") +
          `parse${type.name}`;
      } else {
        fn = parserMapping[type.schema]?.[type.name];
      }

      const commentedValue = `${value} /* ${type.schema}.${type.name} */`;

      if (!fn) {
        fn = `${helperPrefix}.parsePgCatalogValue.bind(${helperPrefix}, ${JSON.stringify(type.oid)})`;
      }

      if (arrayDimensions !== undefined && arrayDimensions > 0) {
        return `${helperPrefix}.parseItemWithinMultiDimArray(${fn}, ${commentedValue})`;
      }

      return `${fn}(${commentedValue})`;
    };

    for (const node of nodes) {
      if (node.kind === "enum") {
        lines.push(
          `export function parse${node.data.name}(v: unknown): ${node.data.name} {`,
          `  switch(v) {`,
        );

        node.data.values.forEach((value) => {
          const enumValue = `${node.data.name}.${value.name}`;
          lines.push(`    case ${enumValue}:`);
        });

        if (node.data.values.length > 0) {
          lines.push(`      return v;`);
        }

        lines.push(
          `    default: throw new Error('Invalid value for enum ${node.data.name}: ' + v);`,
          `  }`,
          `}`,
        );
      } else if (node.kind === "composite") {
        lines.push(
          `export function parse${node.data.name}(u: unknown): ${node.data.name} {`,
          `  const v = ${helperPrefix}.expectIndexed(u);`,
          `  return {`,
        );
        node.data.attributes.forEach((attr, i) => {
          lines.push(`    ${attr.name}: ${renderValue(attr.type, `v[${i}]`, attr.arrayDimensions)},`);
        });
        lines.push(
          `  };`,
          `}`,
        );
      } else if (node.kind === "domain") {
        lines.push(
          `export function parse${node.data.name}(v: unknown): ${node.data.name} {`,
        );
        lines.push(`  return ${renderValue(node.data.type, "v")};`);
        lines.push(`}`);
      }
    }

    lines.push(`export default {`);
    for (const node of nodes) {
      lines.push(`${node.original.name}: parse${node.data.name},`);
    }
    lines.push(`}`);

    return [schema, lines];
  });
}

export function generateKyselySerializers(
  {
    nodes,
    helperImportLocation,
    schemasImportDirectory,
    helperPrefix = "$$",
  }: {
    nodes: PgTypeNode[];
    helperImportLocation: string;
    schemasImportDirectory: string;
    helperPrefix?: string;
  },
): Record<string, string[]> {
  const userDefinedMap = buildUserDefinedMap(nodes.filter((node) => node.kind !== "table" && node.kind !== "enum"));
  const nodesBySchema = groupBy(nodes, (node) => node.data.schema);
  const schemaToVariableMap = mapEntries(
    nodesBySchema,
    ([schema, _]) => [schema, helperPrefix + toValidVariableName(schema)],
  );

  function isUserDefined(type: PgType) {
    return userDefinedMap.get(type.schema)?.has(type.name);
  }

  return mapEntries(nodesBySchema, ([schema, nodes]) => {
    assertExists(nodes);

    const lines: string[] = [
      `import {${nodes.filter((node) => node.kind !== "table").map((node) => node.data.name).join(",")}} from ${
        JSON.stringify(`${schemasImportDirectory.replace(/\/+$/, "")}/${schema}.ts`)
      };`,
      `import * as ${helperPrefix} from ${JSON.stringify(helperImportLocation)};`,
      `import type {${defaultHelperTypes.join(", ")}} from ${JSON.stringify(helperImportLocation)};`,
    ];

    const otherSchemas = Object.keys(nodesBySchema).filter((s) => s !== schema);

    lines.push.apply(
      lines,
      otherSchemas.map((otherSchema) => {
        return `import * as ${schemaToVariableMap[otherSchema]} from ${JSON.stringify(`./${otherSchema}.ts`)};`;
      }),
    );

    const renderValue = (type: PgType, value: string, arrayDimensions?: number) => {
      let fn: string | undefined;

      if (isUserDefined(type)) {
        fn = ((schema !== type.schema) ? `${schemaToVariableMap[type.schema]}.` : "") +
          `serialize${type.name}`;
      } else {
        fn = `${helperPrefix}.serializePgValue`;
      }

      if (arrayDimensions !== undefined && arrayDimensions > 0) {
        return `${helperPrefix}.serializeMultiDimArray(${fn}, ${arrayDimensions}, ${value})`;
      }

      return `${fn}(${value})`;
    };

    for (const node of nodes) {
      if (node.kind === "composite") {
        lines.push(
          `export function serialize${node.data.name}(v: ${node.data.name}): string {`,
          `  return '(' + ([${
            node.data.attributes.map((attr) => {
              return renderValue(attr.type, `v.${attr.name}`, attr.arrayDimensions);
            }).join(", ")
          }].join(",")) + ')'`,
          `}`,
        );
      } else if (node.kind === "domain") {
        lines.push(
          `export function serialize${node.data.name}(v: ${node.data.name}): string {`,
        );
        lines.push(`  return ${renderValue(node.data.type, "v")};`);
        lines.push(`}`);
      }
    }

    const renderColumnSerializer = (column: PgTableColumnType) => {
      const fn = `serialize${column.type.name}`;
      if (column.arrayDimensions !== undefined && column.arrayDimensions > 0) {
        return `${helperPrefix}.serializeMultiDimArray.bind(${helperPrefix}, ${fn}, ${column.arrayDimensions})`;
      }
      return fn;
    };

    lines.push(`export default {`);
    for (const node of nodes) {
      if (node.kind === "table") {
        const columnLines: string[] = [];
        let columnIndex = 0;
        for (const column of node.data.columns) {
          if (isUserDefined(column.type)) {
            const originalColumn = node.original.columns[columnIndex];
            const orignalType = `${originalColumn.type.schema}.${originalColumn.type.name}`;
            const originalTypeArray = originalColumn.arrayDimensions !== undefined
              ? `${orignalType}` + "[]".repeat(originalColumn.arrayDimensions)
              : orignalType;

            columnLines.push(
              `    ${originalColumn.name}: ${helperPrefix}.createSerializerWithTypeCast(${
                renderColumnSerializer(column)
              }, ${JSON.stringify(originalTypeArray)}),`,
            );
          }
          columnIndex++;
        }
        if (columnLines.length > 0) {
          lines.push(`  ${node.original.name}: {`);
          lines.push.apply(lines, columnLines);
          lines.push(`},`);
        }
      }
    }
    lines.push(`}`);

    return [schema, lines];
  });
}
