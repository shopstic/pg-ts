import { PgCatalogOid, pgDecoders } from "../deps.ts";
export type {
  Box as PgBox,
  Circle as PgCircle,
  Float8 as PgFloat8,
  Line as PgLine,
  LineSegment as PgLineSegment,
  Path as PgPath,
  Point as PgPoint,
  Polygon as PgPolygon,
  TID as PgTid,
} from "../deps.ts";

export function expectIndexed(v: unknown): Array<unknown> | Record<string | number, unknown> {
  if (typeof v !== "object" || v === null) throw new Error(`Expected an object, instead got: ${v}`);
  // deno-lint-ignore no-explicit-any
  return v as any;
}

// deno-lint-ignore no-explicit-any
export function parseItemWithinMultiDimArray(parse: (v: unknown) => unknown, array: unknown): any[] {
  if (!Array.isArray(array)) {
    throw new Error("Expected an array, instead got: " + array);
  }

  return array.map((sub) => {
    if (Array.isArray(sub)) return parseItemWithinMultiDimArray(parse, sub);
    return parse(sub);
  });
}

// deno-lint-ignore no-explicit-any
export function createSerializerWithTypeCast(fn: (value: any) => string, type: string) {
  // deno-lint-ignore no-explicit-any
  return (value: any, cast?: boolean) => {
    const ret = fn(value);
    if (cast) {
      return `'${ret.replaceAll("'", "\\'")}'::${type}`;
    }
    return ret;
  };
}

export function serializeMultiDimArray(
  // deno-lint-ignore no-explicit-any
  serializeItem: (v: any) => string,
  arrayDimensions: number,
  array: unknown,
): string {
  if (!Array.isArray(array)) {
    throw new Error("Expected an array, instead got: " + JSON.stringify(array));
  }

  if (arrayDimensions === 1) {
    return "{" + array.map((item) => JSON.stringify(serializeItem(item))).join(",") + "}";
  }

  return "{" + array.map((sub) => {
    return serializeMultiDimArray(serializeItem, arrayDimensions - 1, sub);
  }) + "}";
}

// Mostly taken and modified from https://github.com/denodrivers/postgres/blob/main/query/decode.ts
// deno-lint-ignore no-explicit-any
export function parsePgCatalogValue(typeOid: number, value: unknown): any {
  if (typeof value !== "string") {
    return value;
  }

  switch (typeOid) {
    case PgCatalogOid.bpchar_array:
    case PgCatalogOid.char_array:
    case PgCatalogOid.cidr_array:
    case PgCatalogOid.float4_array:
    case PgCatalogOid.float8_array:
    case PgCatalogOid.inet_array:
    case PgCatalogOid.macaddr_array:
    case PgCatalogOid.name_array:
    case PgCatalogOid.numeric_array:
    case PgCatalogOid.oid_array:
    case PgCatalogOid.regclass_array:
    case PgCatalogOid.regconfig_array:
    case PgCatalogOid.regdictionary_array:
    case PgCatalogOid.regnamespace_array:
    case PgCatalogOid.regoper_array:
    case PgCatalogOid.regoperator_array:
    case PgCatalogOid.regproc_array:
    case PgCatalogOid.regprocedure_array:
    case PgCatalogOid.regrole_array:
    case PgCatalogOid.regtype_array:
    case PgCatalogOid.text_array:
    case PgCatalogOid.time_array:
    case PgCatalogOid.timetz_array:
    case PgCatalogOid.uuid_array:
    case PgCatalogOid.varchar_array:
      return pgDecoders.decodeStringArray(value);
    case PgCatalogOid.int2:
    case PgCatalogOid.int4:
    case PgCatalogOid.xid:
      return pgDecoders.decodeInt(value);
    case PgCatalogOid.int2_array:
    case PgCatalogOid.int4_array:
    case PgCatalogOid.xid_array:
      return pgDecoders.decodeIntArray(value);
    case PgCatalogOid.bool:
      return pgDecoders.decodeBoolean(value);
    case PgCatalogOid.bool_array:
      return pgDecoders.decodeBooleanArray(value);
    case PgCatalogOid.box:
      return pgDecoders.decodeBox(value);
    case PgCatalogOid.box_array:
      return pgDecoders.decodeBoxArray(value);
    case PgCatalogOid.circle:
      return pgDecoders.decodeCircle(value);
    case PgCatalogOid.circle_array:
      return pgDecoders.decodeCircleArray(value);
    case PgCatalogOid.bytea:
      return pgDecoders.decodeBytea(value);
    case PgCatalogOid.byte_array:
      return pgDecoders.decodeByteaArray(value);
    case PgCatalogOid.date:
      return pgDecoders.decodeDate(value);
    case PgCatalogOid.date_array:
      return pgDecoders.decodeDateArray(value);
    case PgCatalogOid.int8:
      return pgDecoders.decodeBigint(value);
    case PgCatalogOid.int8_array:
      return pgDecoders.decodeBigintArray(value);
    case PgCatalogOid.json:
    case PgCatalogOid.jsonb:
      return pgDecoders.decodeJson(value);
    case PgCatalogOid.json_array:
    case PgCatalogOid.jsonb_array:
      return pgDecoders.decodeJsonArray(value);
    case PgCatalogOid.line:
      return pgDecoders.decodeLine(value);
    case PgCatalogOid.line_array:
      return pgDecoders.decodeLineArray(value);
    case PgCatalogOid.lseg:
      return pgDecoders.decodeLineSegment(value);
    case PgCatalogOid.lseg_array:
      return pgDecoders.decodeLineSegmentArray(value);
    case PgCatalogOid.path:
      return pgDecoders.decodePath(value);
    case PgCatalogOid.path_array:
      return pgDecoders.decodePathArray(value);
    case PgCatalogOid.point:
      return pgDecoders.decodePoint(value);
    case PgCatalogOid.point_array:
      return pgDecoders.decodePointArray(value);
    case PgCatalogOid.polygon:
      return pgDecoders.decodePolygon(value);
    case PgCatalogOid.polygon_array:
      return pgDecoders.decodePolygonArray(value);
    case PgCatalogOid.tid:
      return pgDecoders.decodeTid(value);
    case PgCatalogOid.tid_array:
      return pgDecoders.decodeTidArray(value);
    case PgCatalogOid.timestamp:
    case PgCatalogOid.timestamptz:
      return pgDecoders.decodeDatetime(value);
    case PgCatalogOid.timestamp_array:
    case PgCatalogOid.timestamptz_array:
      return pgDecoders.decodeDatetimeArray(value);
    default:
      return value;
  }
}

function pad(number: number, digits: number): string {
  return String(number).padStart(digits, "0");
}

// Taken from https://github.com/denodrivers/postgres/blob/main/query/encode.ts
// since it's unfortunately not exported
export function serializePgDate(date: Date): string {
  // Construct ISO date
  const year = pad(date.getFullYear(), 4);
  const month = pad(date.getMonth() + 1, 2);
  const day = pad(date.getDate(), 2);
  const hour = pad(date.getHours(), 2);
  const min = pad(date.getMinutes(), 2);
  const sec = pad(date.getSeconds(), 2);
  const ms = pad(date.getMilliseconds(), 3);

  const encodedDate = `${year}-${month}-${day}T${hour}:${min}:${sec}.${ms}`;

  // Construct timezone info
  //
  // Date.prototype.getTimezoneOffset();
  //
  // From MDN:
  // > The time-zone offset is the difference, in minutes, from local time to UTC.
  // > Note that this means that the offset is positive if the local timezone is
  // > behind UTC and negative if it is ahead. For example, for time zone UTC+10:00
  // > (Australian Eastern Standard Time, Vladivostok Time, Chamorro Standard Time),
  // > -600 will be returned.
  const offset = date.getTimezoneOffset();
  const tzSign = offset > 0 ? "-" : "+";
  const absOffset = Math.abs(offset);
  const tzHours = pad(Math.floor(absOffset / 60), 2);
  const tzMinutes = pad(Math.floor(absOffset % 60), 2);

  const encodedTz = `${tzSign}${tzHours}:${tzMinutes}`;

  return encodedDate + encodedTz;
}

export function serializePgValue(value: unknown): string {
  if (value === undefined || value === null) {
    return "NULL";
  }

  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
    return JSON.stringify(value);
  }

  if (typeof value === "bigint") {
    return value.toString();
  }

  if (value instanceof Date) {
    return serializePgDate(value);
  }

  throw new Error(`Unsupported serialization for value ${value} of type ${typeof value}`);
}
