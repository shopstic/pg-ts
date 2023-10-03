export interface PgType {
  schema: string;
  oid: number;
  name: string;
}

export interface PgField {
  name: string;
  type: PgType;
}

export interface PgObject {
  name: string;
  schema: string;
}

export interface PgObject {
  schema: string;
  name: string;
}

export interface PgCompositeAttributeType extends PgField {
  order: number;
  arrayDimensions?: number;
}

export interface PgCompositeType extends PgObject {
  attributes: PgCompositeAttributeType[];
}

export interface PgDomainType extends PgObject {
  type: PgType;
  constraint?: string;
}

export interface PgEnumValueType {
  name: string;
  order: number;
}

export interface PgEnumType extends PgObject {
  values: PgEnumValueType[];
}

export interface PgTableColumnType extends PgField {
  order: number;
  isNullable: boolean;
  defaultValue?: string;
  arrayDimensions?: number;
}

export interface PgTableType extends PgObject {
  columns: PgTableColumnType[];
}

export interface PgTypes {
  composites: PgCompositeType[];
  domains: PgDomainType[];
  enums: PgEnumType[];
  tables: PgTableType[];
}

export type PgTypeNodeKind = "composite" | "domain" | "enum" | "table";
export type PgTypeNode =
  | { kind: "composite"; dependencies: Set<string>; data: PgCompositeType; original: PgCompositeType }
  | { kind: "domain"; dependencies: Set<string>; data: PgDomainType; original: PgDomainType }
  | { kind: "enum"; dependencies: Set<string>; data: PgEnumType; original: PgEnumType }
  | { kind: "table"; dependencies: Set<string>; data: PgTableType; original: PgTableType };
