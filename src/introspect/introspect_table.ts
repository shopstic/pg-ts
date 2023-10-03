import { PgClient } from "../deps.ts";
import { PgTableType } from "../types.ts";

type PgTableIntrospectionResult = {
  table_schema: string;
  table_name: string;
  column_name: string;
  column_num: string;
  column_type_name: string;
  column_type_schema: string;
  column_type_oid: string;
  column_is_nullable: string;
  column_default_value: string;
  column_array_dimensions: string;
};

async function query(client: PgClient, schemas: string[]) {
  const query = `
    SELECT
        ns.nspname AS table_schema,
        tbl.relname AS table_name,
        attr.attname AS column_name,
        attr.attnum AS column_num,
        CASE
            WHEN attr.attndims > 0 THEN right(col_type.typname, -1)  -- Remove '_' prefix for array types
            ELSE col_type.typname
        END AS column_type_name,  -- Use the conditional on attndims to determine if we remove the '_' prefix
        col_type.oid AS column_type_oid,
        type_n.nspname AS column_type_schema,
        CASE
            WHEN attr.attnotnull THEN 'false'
            ELSE 'true'
        END AS column_is_nullable,
        pg_get_expr(ad.adbin, attr.attrelid) AS column_default_value,
        attr.attndims AS column_array_dimensions
    FROM
        pg_attribute attr
    LEFT JOIN
        pg_attrdef ad ON attr.attrelid = ad.adrelid AND attr.attnum = ad.adnum
    JOIN
        pg_class tbl ON attr.attrelid = tbl.oid
    JOIN
        pg_namespace ns ON tbl.relnamespace = ns.oid
    JOIN
        pg_type col_type ON attr.atttypid = col_type.oid  -- Joined for the data type
    JOIN
        pg_namespace type_n ON col_type.typnamespace = type_n.oid  -- Joined for the data type schema
    WHERE
        attr.attnum > 0 AND NOT attr.attisdropped
        AND tbl.relkind IN ('r', 'p')
        AND ns.nspname = ANY($1::text[])
  `;

  return (await client.queryObject(query, [schemas])).rows as PgTableIntrospectionResult[];
}

function convert(results: PgTableIntrospectionResult[]): PgTableType[] {
  const bySchemaMap = new Map<string, Map<string, PgTableType>>();
  const ret: PgTableType[] = [];

  for (const result of results) {
    let bySchema = bySchemaMap.get(result.table_schema);
    if (!bySchema) {
      bySchema = new Map();
      bySchemaMap.set(result.table_schema, bySchema);
    }
    let table = bySchema.get(result.table_name);

    if (!table) {
      table = {
        name: result.table_name,
        schema: result.table_schema,
        columns: [],
      };
      bySchema.set(table.name, table);
      ret.push(table);
    }

    const arrayDimensions = parseInt(result.column_array_dimensions, 10);
    const columnTypeName = arrayDimensions > 0 ? result.column_type_name.replace(/\[\]$/, "") : result.column_type_name;

    table.columns.push({
      name: result.column_name,
      type: {
        schema: result.column_type_schema,
        name: columnTypeName,
        oid: parseInt(result.column_type_oid),
      },
      order: parseInt(result.column_num, 10),
      isNullable: result.column_is_nullable === "true",
      defaultValue: result.column_default_value ?? undefined,
      arrayDimensions: arrayDimensions > 0 ? arrayDimensions : undefined,
    });
  }

  for (const table of ret) {
    table.columns.sort((a, b) => a.order - b.order);
  }

  return ret;
}

export async function introspectPgTables(
  client: PgClient,
  schemas = ["public"],
): Promise<PgTableType[]> {
  const rows = await query(client, schemas);
  return convert(rows);
}
