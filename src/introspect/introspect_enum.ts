import { PgClient } from "../deps.ts";
import { PgEnumType } from "../types.ts";

type PgEnumIntrospectionResult = {
  type_name: string;
  type_schema: string;
  value_name: string;
  value_order: string;
};

async function query(client: PgClient, schemas: string[]) {
  const query = `
    SELECT
      t.typname as type_name,
      n.nspname as type_schema,
      e.enumlabel as value_name,
      e.enumsortorder as value_order
    FROM
      pg_catalog.pg_type t
      JOIN pg_catalog.pg_enum e ON t.oid = e.enumtypid
      JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
    WHERE
      n.nspname = ANY($1::text[])
  `;

  return (await client.queryObject(query, [schemas])).rows as PgEnumIntrospectionResult[];
}

function convert(results: PgEnumIntrospectionResult[]): PgEnumType[] {
  const bySchemaMap = new Map<string, Map<string, PgEnumType>>();
  const ret: PgEnumType[] = [];

  for (const result of results) {
    let bySchema = bySchemaMap.get(result.type_schema);
    if (!bySchema) {
      bySchema = new Map();
      bySchemaMap.set(result.type_schema, bySchema);
    }
    let type = bySchema.get(result.type_name);

    if (!type) {
      type = {
        name: result.type_name,
        schema: result.type_schema,
        values: [],
      };
      bySchema.set(type.name, type);
      ret.push(type);
    }

    type.values.push({
      name: result.value_name,
      order: parseInt(result.value_order, 10),
    });
  }

  for (const type of ret) {
    type.values.sort((a, b) => a.order - b.order);
  }

  return ret;
}

export async function introspectPgEnums(
  client: PgClient,
  schemas = ["public"],
): Promise<PgEnumType[]> {
  const rows = await query(client, schemas);
  return convert(rows);
}
