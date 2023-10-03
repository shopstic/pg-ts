import {
  ClientOptions,
  ConnectionString,
  createPool,
  IPostgresPool,
  IPostgresPoolClient,
  IPostgresQueryResult,
  PgClient,
  PgColumn,
  Pool,
  PoolOptions,
} from "../deps.ts";
import { AnsiColors } from "../deps/cliffy.ts";
import { parsePgObject } from "../serdes/parse_pg_object.ts";
import { parseItemWithinMultiDimArray } from "./kysely_helpers.ts";

type ColumnParser = {
  isArray: boolean;
  parse: (value: unknown) => unknown;
};

type PoolItem = {
  client: PgClient;
  parsers: Map<number, ColumnParser>;
};

type NamedColumnParser = {
  column: PgColumn;
  parser: ColumnParser;
};

class PostgresPoolClient {
  constructor(readonly underlying: PoolItem, readonly onRelease: () => void) {}
  async query<R>(sql: string, parameters: ReadonlyArray<unknown>): Promise<IPostgresQueryResult<R>> {
    const ret = await this.underlying.client.queryObject(sql, parameters as unknown[]);

    const columnParsers: NamedColumnParser[] = [];

    if (ret.rowDescription) {
      for (const column of ret.rowDescription.columns) {
        const parser = this.underlying.parsers.get(column.typeOid);
        if (parser !== undefined) {
          columnParsers.push({
            column,
            parser,
          });
        }
      }

      if (columnParsers.length > 0) {
        // deno-lint-ignore no-explicit-any
        let row: any;
        for (row of ret.rows) {
          for (const columnParser of columnParsers) {
            let parsed: unknown;
            const colName = columnParser.column.name;
            const rawValue = row[colName];

            try {
              parsed = parsePgObject(rawValue);

              const parser = columnParser.parser;
              if (parser.isArray) {
                if (!Array.isArray(parsed)) {
                  throw new Error(
                    `Column ${JSON.stringify(colName)} parser expected an array value`,
                  );
                }
                row[colName] = parseItemWithinMultiDimArray(parser.parse, parsed);
              } else {
                row[colName] = parser.parse(parsed);
              }
            } catch (e) {
              const errorTag = AnsiColors.bold.brightRed("error");
              console.error(
                errorTag,
                "Failed parsing column",
                AnsiColors.underline(colName),
                "value using the provided custom parser",
              );
              console.error(errorTag, "Column definition", columnParser.column);
              console.error(errorTag, "Raw value", rawValue);
              throw e;
            }
          }
        }
      }
    }

    return ret as IPostgresQueryResult<R>;
  }
  release(): void {
    this.onRelease();
  }
}

// deno-lint-ignore no-explicit-any
const poolProto = Pool.prototype as any;
poolProto._scheduleEvictorRun = function () {
  if (this._config.evictionRunIntervalMillis > 0) {
    this._scheduledEviction = setTimeout(() => {
      this._evict();
      this._scheduleEvictorRun();
    }, this._config.evictionRunIntervalMillis);
  }
};

type CustomTypeOids = {
  oid: number;
  arrayOid: number;
};

async function introspectCustomTypes(
  client: PgClient,
  excludeSchemas: string[],
) {
  const query = `
    SELECT 
        n.nspname,
        t.typname,
        t.typcategory,
        t.oid
    FROM pg_type t
        JOIN pg_namespace n ON t.typnamespace = n.oid
        LEFT OUTER JOIN pg_class c ON c.oid = t.typrelid
        LEFT OUTER JOIN pg_type elem_type ON elem_type.oid = t.typelem
        LEFT OUTER JOIN pg_class elem_class ON elem_class.oid = elem_type.typrelid
    WHERE NOT (n.nspname = ANY($1::text[]))
    AND c.relkind IS DISTINCT FROM 'S'
    AND c.relkind IS DISTINCT FROM 'r'
    AND (
        t.typtype IN ('c', 'e', 'd') OR
        (t.typtype = 'b' AND t.typcategory = 'A' AND elem_class.relkind IS DISTINCT FROM 'r')
    );
  `;

  const rows = (await client.queryObject(query, [excludeSchemas])).rows;
  const mapping = new Map<string, Map<string, CustomTypeOids>>();

  // deno-lint-ignore no-explicit-any
  let row: any;
  for (row of rows) {
    const oid = Number(row.oid);
    const isArray = row.typcategory === "A";
    const name = isArray ? row.typname.slice(1) : row.typname;
    let bySchema = mapping.get(row.nspname);

    if (!bySchema) {
      bySchema = new Map();
      mapping.set(row.nspname, bySchema);
    }

    let oids = bySchema.get(name);

    if (!oids) {
      oids = {
        oid: -1,
        arrayOid: -1,
      };
      bySchema.set(name, oids);
    }

    if (isArray) {
      oids.arrayOid = oid;
    } else {
      oids.oid = oid;
    }
  }

  return mapping;
}

export class PostgresPool implements IPostgresPool {
  readonly pool: Pool<PoolItem>;

  constructor(
    {
      connection: connectionParams,
      pool: poolOptions,
      parsers: {
        instrospectionExcludeSchemas = ["hdb_catalog", "pg_catalog", "information_schema", "pg_toast"],
        mapping: parsersMapping,
      } = {},
    }: {
      connection: ClientOptions | ConnectionString | undefined;
      pool: PoolOptions & { destroyTimeoutMillis?: number };
      parsers?: {
        instrospectionExcludeSchemas?: string[];
        mapping?: Record<string, Record<string, (value: unknown) => unknown>>;
      };
    },
  ) {
    this.pool = createPool({
      async create() {
        const client = new PgClient(connectionParams);
        await client.connect();

        const parserByOidMap = new Map<number, ColumnParser>();

        if (parsersMapping) {
          const oidsMap = await introspectCustomTypes(client, instrospectionExcludeSchemas);

          for (const [schema, map] of oidsMap.entries()) {
            const parsersInSchema = parsersMapping[schema];

            if (parsersInSchema) {
              for (const [name, oids] of map.entries()) {
                const fn = parsersInSchema[name];
                if (fn) {
                  const parse = (v: unknown) => {
                    if (typeof v === "string") {
                      return fn(parsePgObject(v));
                    }
                    return fn(v);
                  };
                  parserByOidMap.set(oids.oid, {
                    isArray: false,
                    parse,
                  });
                  parserByOidMap.set(oids.arrayOid, {
                    isArray: true,
                    parse,
                  });
                }
              }
            }
          }
        }

        return {
          client,
          parsers: parserByOidMap,
        };
      },
      destroy(item) {
        return item.client.end();
      },
      validate() {
        return Promise.resolve(true);
      },
    }, poolOptions);
  }

  async connect(): Promise<IPostgresPoolClient> {
    const underlying = await this.pool.acquire();
    const client = new PostgresPoolClient(underlying, () => this.pool.release(underlying));
    return client as unknown as IPostgresPoolClient;
  }

  end(): Promise<void> {
    return this.pool.drain();
  }
}
