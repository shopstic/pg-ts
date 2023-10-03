import { PgClient } from "../deps.ts";
import { PgCompositeType } from "../types.ts";

type PgCompositeIntrospectionResult = {
  type_name: string;
  type_schema: string;
  attribute_num: string;
  attribute_name: string;
  attribute_type_name: string;
  attribute_type_oid: string;
  attribute_type_schema: string;
  attribute_array_dimensions: string;
};

async function query(client: PgClient, schemas: string[]) {
  const query = `
    SELECT
      t.typname           as type_name,
      n.nspname           as type_schema,
      a.attnum            as attribute_num,
      a.attname           as attribute_name,
      CASE WHEN a.attndims > 0 THEN substring(attr_t.typname, 2) ELSE attr_t.typname END as attribute_type_name,
      a.atttypid          as attribute_type_oid,
      attr_n.nspname      as attribute_type_schema,
      a.attndims          as attribute_array_dimensions
    FROM
      pg_type t
          JOIN
      pg_namespace n ON t.typnamespace = n.oid
          JOIN
      pg_class c ON c.oid = t.typrelid
          JOIN
      pg_attribute a ON a.attnum > 0 AND a.attrelid = t.typrelid
          JOIN
      pg_type attr_t ON a.atttypid = attr_t.oid
          JOIN
      pg_namespace attr_n ON attr_t.typnamespace = attr_n.oid
    WHERE
      n.nspname = ANY($1::text[])   -- limit to specified schemas
      AND t.typtype = 'c'              -- 'c' for composite types
      AND c.relkind NOT IN ('r', 'S')  -- exclude ordinary tables and sequencesbles and sequences
  `;

  return (await client.queryObject(query, [schemas])).rows as PgCompositeIntrospectionResult[];
}

function convert(results: PgCompositeIntrospectionResult[]): PgCompositeType[] {
  const bySchemaMap = new Map<string, Map<string, PgCompositeType>>();
  const ret: PgCompositeType[] = [];

  for (const result of results) {
    let bySchema = bySchemaMap.get(result.type_schema);

    if (!bySchema) {
      bySchema = new Map();
      bySchemaMap.set(result.type_schema, bySchema);
    }

    let composite = bySchema.get(result.type_name);

    if (!composite) {
      composite = {
        name: result.type_name,
        schema: result.type_schema,
        attributes: [],
      };
      bySchema.set(composite.name, composite);
      ret.push(composite);
    }

    const arrayDimensions = parseInt(result.attribute_array_dimensions, 10);
    const attributeTypeName = arrayDimensions > 0
      ? result.attribute_type_name.replace(/\[\]$/, "")
      : result.attribute_type_name;

    composite.attributes.push({
      name: result.attribute_name,
      type: {
        name: attributeTypeName,
        schema: result.attribute_type_schema,
        oid: parseInt(result.attribute_type_oid),
      },
      order: parseInt(result.attribute_num),
      arrayDimensions: arrayDimensions,
    });
  }

  for (const type of ret) {
    type.attributes.sort((a, b) => a.order - b.order);
  }

  return ret;
}

export async function introspectPgCompositeTypes(
  client: PgClient,
  schemas = ["public"],
): Promise<PgCompositeType[]> {
  const rows = await query(client, schemas);
  return convert(rows);
}
