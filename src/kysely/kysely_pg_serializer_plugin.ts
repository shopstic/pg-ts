import {
  AliasNode,
  BinaryOperationNode,
  ColumnNode,
  ColumnUpdateNode,
  DeleteQueryNode,
  IdentifierNode,
  InsertQueryNode,
  KyselyPlugin,
  OperationNode,
  OperationNodeTransformer,
  PluginTransformQueryArgs,
  PluginTransformResultArgs,
  PrimitiveValueListNode,
  QueryResult,
  RawNode,
  ReferenceNode,
  RootOperationNode,
  SelectQueryNode,
  TableNode,
  UnknownRow,
  UpdateQueryNode,
  ValueListNode,
  ValueNode,
  ValuesNode,
  WhereNode,
} from "../deps.ts";

// deno-lint-ignore no-explicit-any
type Serializer = (value: any, cast?: boolean) => string;
type Writeable<T> = { -readonly [P in keyof T]: T[P] };
// type SchemableIdentifierNode = ReturnType<OperationNodeTransformer["transformSchemableIdentifier"]>;

function writeable<T>(value: T) {
  return value as Writeable<T>;
}

type SerializerAccessor = (table: string, column: string, schema?: string) => Serializer | undefined;

class Transformer extends OperationNodeTransformer {
  private currentTables: { schema?: string; name: string; alias?: string }[] = [];

  constructor(private getSerializer: SerializerAccessor) {
    super();
  }

  public initBeforeQuery() {
    this.currentTables = [];
    return this;
  }

  private extractCurrentTables(node: OperationNode) {
    if (AliasNode.is(node)) {
      const tableNode = node.node;

      if (TableNode.is(tableNode)) {
        this.currentTables.push({
          name: tableNode.table.identifier.name,
          schema: tableNode.table.schema?.name,
          alias: IdentifierNode.is(node.alias) ? node.alias.name : undefined,
        });
      }
    } else if (TableNode.is(node)) {
      this.currentTables.push({
        name: node.table.identifier.name,
        schema: node.table.schema?.name,
      });
    } else {
      throw new Error("Expected AliasNode or TableNode, instead got node.kind: " + node.kind);
    }
  }

  protected override transformSelectQuery(node: SelectQueryNode): SelectQueryNode {
    if (node.from) {
      for (const from of node.from.froms) {
        this.extractCurrentTables(from);
      }
    }

    if (node.joins) {
      for (const join of node.joins) {
        this.extractCurrentTables(join.table);
      }
    }

    return super.transformSelectQuery(node);
  }

  protected override transformDeleteQuery(node: DeleteQueryNode): DeleteQueryNode {
    if (node.from) {
      for (const from of node.from.froms) {
        this.extractCurrentTables(from);
      }
    }

    return super.transformDeleteQuery(node);
  }

  protected override transformUpdateQuery(updateQueryNode: UpdateQueryNode): UpdateQueryNode {
    this.extractCurrentTables(updateQueryNode.table);

    return super.transformUpdateQuery(updateQueryNode);
  }

  protected override transformWhere(node: WhereNode): WhereNode {
    if (this.currentTables.length === 0) {
      throw new Error("Expected currentTables to already be non-empty inside transformWhere");
    }

    const where = node.where;
    let leftOp: OperationNode;
    let leftOpCol: OperationNode;

    if (
      BinaryOperationNode.is(where) &&
      (leftOp = where.leftOperand) &&
      ReferenceNode.is(leftOp) &&
      (leftOpCol = leftOp.column) &&
      ColumnNode.is(leftOpCol)
    ) {
      const columnName = leftOpCol.column.name;
      let tableName = leftOp.table?.table.identifier.name;
      let schema = leftOp.table?.table.schema?.name;

      const currentTables = this.currentTables;

      if (!tableName) {
        tableName = currentTables[0].name;
        schema = currentTables[0].schema;
      } else {
        const match = currentTables.find((t) =>
          (t.name === tableName || t.alias === tableName) && (!schema || t.schema === schema)
        );

        if (!match) {
          throw new Error(
            `Expected to find a table with name or alias of ${JSON.stringify(tableName)}${
              schema ? ` and schema of ${JSON.stringify(schema)}` : ""
            }. Current tables: ${JSON.stringify(currentTables)}`,
          );
        }

        tableName = match.name;
        schema = match.schema;
      }

      const serializer = this.getSerializer(tableName, columnName, schema);

      if (serializer) {
        const currentRightOp = where.rightOperand;
        let newRightOp: OperationNode;

        if (PrimitiveValueListNode.is(currentRightOp)) {
          newRightOp = ValueListNode.create(
            currentRightOp.values.map((v) => {
              return RawNode.create([serializer(v, true)], []);
            }),
          );
        } else if (ValueNode.is(currentRightOp)) {
          newRightOp = RawNode.create([
            serializer(currentRightOp.value, true),
          ], []);
        } else {
          throw new Error("Unexpected node.where.rightOperand.kind: " + currentRightOp.kind);
        }

        return super.transformWhere({
          ...node,
          where: {
            ...where,
            rightOperand: newRightOp,
          } as BinaryOperationNode,
        });
      }
    }

    return super.transformWhere(node);
  }

  protected override transformColumnUpdate(node: ColumnUpdateNode): ColumnUpdateNode {
    if (ValueNode.is(node.value)) {
      const currentTables = this.currentTables;

      if (currentTables.length !== 1) {
        throw new Error(
          "Expected exactly one item in currentTables should be set inside transformColumnUpdate, but it's currently:" +
            JSON.stringify(currentTables),
        );
      }

      const nodeValue = node.value;
      const columnName = node.column.column.name;
      const serializer = this.getSerializer(currentTables[0].name, columnName, currentTables[0].schema);

      if (serializer) {
        const newValue: ValueNode = {
          ...nodeValue,
          value: serializer(nodeValue.value),
        };

        return super.transformColumnUpdate({
          ...node,
          value: newValue,
        });
      }
    }

    return super.transformColumnUpdate(node);
  }

  protected override transformInsertQuery(insertQueryNode: InsertQueryNode): InsertQueryNode {
    const columns = insertQueryNode.columns;

    if (columns && insertQueryNode.values) {
      let hasSerializers = false;
      const columnSerializers: Array<Serializer | undefined> = new Array(insertQueryNode.columns.length);
      const intoTable = insertQueryNode.into.table;
      const schema = intoTable.schema?.name;
      const tableName = intoTable.identifier.name;

      this.currentTables.push({
        name: tableName,
        schema,
      });

      for (let i = 0; i < columns.length; i++) {
        const columnName = columns[i].column.name;
        const serializer = this.getSerializer(tableName, columnName, schema);

        if (serializer) {
          hasSerializers = true;
          columnSerializers[i] = serializer;
        }
      }

      if (hasSerializers) {
        const newNode = structuredClone(insertQueryNode);

        if (!newNode.values || !ValuesNode.is(newNode.values)) {
          throw new Error("Expected ValuesNode at this point, got: " + newNode.values?.kind);
        }

        for (const valueList of newNode.values.values) {
          if (!PrimitiveValueListNode.is(valueList)) {
            throw new Error("Expected PrimitiveValueListNode, got: " + valueList.kind);
          }

          const values = writeable(valueList.values);

          for (let i = 0; i < columnSerializers.length; i++) {
            const serializer = columnSerializers[i];
            if (serializer) {
              values[i] = serializer(values[i]);
            }
          }
        }

        return super.transformInsertQuery(newNode);
      }
    }

    return super.transformInsertQuery(insertQueryNode);
  }
}

export class KyselyPgSerializerPlugin implements KyselyPlugin {
  private transformer: Transformer;

  constructor(
    serializerMapping: Record<string, Record<string, Record<string, Serializer>>>,
    defaultSchema = "public",
  ) {
    const getSerializer = (table: string, column: string, schema = defaultSchema) => {
      return serializerMapping[schema]?.[table]?.[column];
    };

    this.transformer = new Transformer(getSerializer);
  }

  transformQuery(args: PluginTransformQueryArgs): RootOperationNode {
    // console.log("transformQuery", JSON.stringify(args, null, 2));
    return this.transformer.initBeforeQuery().transformNode(args.node);
  }

  transformResult(
    args: PluginTransformResultArgs,
  ): Promise<QueryResult<UnknownRow>> {
    return Promise.resolve(args.result);
  }
}
