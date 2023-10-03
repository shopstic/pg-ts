import { PgType, PgTypeNode, PgTypes } from "../types.ts";

export function pgTypesToNodes(pgTypes: PgTypes, sortTopologically = false): PgTypeNode[] {
  const compositeNodes: PgTypeNode[] = pgTypes.composites.map((composite) => ({
    kind: "composite",
    data: composite,
    original: structuredClone(composite),
    dependencies: new Set(),
  }));
  const domainNodes: PgTypeNode[] = pgTypes.domains.map((domain) => ({
    kind: "domain",
    data: domain,
    original: structuredClone(domain),
    dependencies: new Set(),
  }));
  const enumNodes: PgTypeNode[] = pgTypes.enums.map((enumType) => ({
    kind: "enum",
    data: enumType,
    original: structuredClone(enumType),
    dependencies: new Set(),
  }));
  const tableNodes: PgTypeNode[] = pgTypes.tables.map((table) => ({
    kind: "table",
    data: table,
    original: structuredClone(table),
    dependencies: new Set(),
  }));

  const transformed = transform([
    ...compositeNodes,
    ...domainNodes,
    ...enumNodes,
    ...tableNodes,
  ], sortTopologically);

  if (sortTopologically) {
    return topologicalSort(transformed);
  }

  transformed.sort((a, b) => a.data.name.localeCompare(b.data.name));
  return transformed;
}

function toCamelCase(str: string): string {
  let output = "";
  let shouldCapitalize = false;

  for (let i = 0; i < str.length; i++) {
    const char = str[i];

    if (char === "_") {
      shouldCapitalize = true;
    } else {
      output += shouldCapitalize ? char.toUpperCase() : char;
      shouldCapitalize = false;
    }
  }

  return output;
}

function toUpperCamelCase(str: string): string {
  return str.replace(/(?:^|_)([a-z])/g, (_, c) => c.toUpperCase());
}

export function buildUserDefinedMap(pgNodes: PgTypeNode[]) {
  const userDefinedMap = new Map<string, Map<string, PgTypeNode>>();

  for (const node of pgNodes) {
    const schema = node.data.schema;
    const name = node.data.name;

    if (!schema) {
      throw new Error("Missing schema at node " + JSON.stringify(node));
    }

    if (!userDefinedMap.has(schema)) {
      userDefinedMap.set(schema, new Map());
    }

    userDefinedMap.get(schema)!.set(name, node);
  }

  return userDefinedMap;
}

function transform(nodes: PgTypeNode[], trackDependencies = false) {
  const userDefinedMap = buildUserDefinedMap(nodes);

  function isUserDefined(type: PgType) {
    return userDefinedMap.get(type.schema)?.has(type.name);
  }

  function toFullName(type: PgType) {
    return `${type.schema}.${type.name}`;
  }

  for (const node of nodes) {
    node.data.name = toUpperCamelCase(node.data.name);

    if (node.kind === "composite") {
      node.data.attributes.forEach((attribute) => {
        attribute.name = toCamelCase(attribute.name);

        if (isUserDefined(attribute.type)) {
          attribute.type.name = toUpperCamelCase(attribute.type.name);
          if (trackDependencies) node.dependencies.add(toFullName(attribute.type));
        }
      });
    } else if (node.kind === "table") {
      node.data.columns.forEach((column) => {
        column.name = toCamelCase(column.name);

        if (isUserDefined(column.type)) {
          column.type.name = toUpperCamelCase(column.type.name);
          if (trackDependencies) node.dependencies.add(toFullName(column.type));
        }
      });
    } else if (node.kind === "domain") {
      if (isUserDefined(node.data.type)) {
        node.data.type.name = toUpperCamelCase(node.data.type.name);
        if (trackDependencies) node.dependencies.add(toFullName(node.data.type));
      }
    }
  }

  return nodes;
}

function topologicalSort(nodes: PgTypeNode[]): PgTypeNode[] {
  const visited = new Set<string>();
  const stack: PgTypeNode[] = [];

  // Helper function: perform Depth First Search from a given node
  function dfs(node: PgTypeNode) {
    // Mark the current node as visited
    visited.add(`${node.data.schema}.${node.data.name}`);

    // Go through all dependencies of this node
    node.dependencies.forEach((dep) => {
      // If the dependency is not visited yet, visit it
      if (!visited.has(dep)) {
        const depNode = nodes.find((n) => n.data.name === dep);
        if (depNode) {
          dfs(depNode);
        }
      }
    });

    // Push the current node to the stack
    stack.push(node);
  }

  // Go through all nodes
  nodes.forEach((node) => {
    // If the node is not visited yet, start DFS from it
    if (!visited.has(node.data.name)) {
      dfs(node);
    }
  });

  return stack;
}
