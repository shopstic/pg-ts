import { PgClient } from "../deps.ts";
import { PgDomainType } from "../types.ts";

type PgDomainIntrospectionResult = {
  name: string;
  schema: string;
  type_oid: string;
  type_name: string;
  type_schema: string;
  constraint: string | null;
};

export async function introspectPgDomains(
  client: PgClient,
  schemas = ["public"],
): Promise<PgDomainType[]> {
  const rows = await query(client, schemas);
  return convert(rows);
}

async function query(
  client: PgClient,
  schemas: string[],
): Promise<PgDomainIntrospectionResult[]> {
  const query = `
    SELECT t.typname                             AS name,
          n.nspname                              AS schema,
          d.typname                              as type_name,
          d.oid                                  AS type_oid,
          under_n.nspname                        AS type_schema,
          pg_catalog.pg_get_constraintdef(c.oid) AS constraint
    FROM pg_catalog.pg_type t
            JOIN
        pg_catalog.pg_namespace n ON t.typnamespace = n.oid
            LEFT JOIN
        pg_catalog.pg_constraint c ON t.oid = c.contypid
            JOIN
        pg_catalog.pg_type d ON t.typbasetype = d.oid
            JOIN
        pg_catalog.pg_namespace under_n ON d.typnamespace = under_n.oid
    WHERE n.nspname = ANY ($1::text[])
      AND t.typtype = 'd'
  `;

  return (await client.queryObject(query, [schemas])).rows as PgDomainIntrospectionResult[];
}

function convert(results: PgDomainIntrospectionResult[]): PgDomainType[] {
  const bySchemaMap = new Map<string, Map<string, PgDomainType>>();
  const ret: PgDomainType[] = [];

  for (const result of results) {
    let bySchema = bySchemaMap.get(result.schema);
    if (!bySchema) {
      bySchema = new Map();
      bySchemaMap.set(result.schema, bySchema);
    }
    let domain = bySchema.get(result.name);

    if (!domain) {
      domain = {
        name: result.name,
        schema: result.schema,
        type: {
          oid: parseInt(result.type_oid),
          name: result.type_name,
          schema: result.type_schema,
        },
        constraint: result.constraint !== null ? result.constraint : undefined,
      };
      bySchema.set(domain.name, domain);
      ret.push(domain);
    }
  }

  return ret;
}
