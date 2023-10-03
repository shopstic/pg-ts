import { assertEquals, assertThrows } from "../deps.ts";
import { parsePgObject } from "./parse_pg_object.ts";

// Tests for malformed inputs
Deno.test("should throw an error on malformed input (unterminated object)", () => {
  assertThrows(() => parsePgObject(`("a", "b",`));
});

Deno.test("should throw an error on malformed input (unterminated string)", () => {
  assertThrows(() => parsePgObject(`("a", "b"`));
});

Deno.test("should parse a simple string", () => {
  assertEquals(parsePgObject("hello"), "hello");
  assertEquals(parsePgObject("(hello world)"), { "0": "hello", "1": "world" });
  assertEquals(parsePgObject('{hello "world"}'), ["hello", "world"]);
  assertEquals(parsePgObject('"hello"'), "hello");
});

// Test for numbers
Deno.test("should correctly parse integers and floats", () => {
  const input = `{1, 2.2, .3, 4e5, 5.67e+8}`;
  const expected = [1, 2.2, 0.3, 400000, 567000000];
  const result = parsePgObject(input);
  assertEquals(result, expected);
});

// Tests for booleans
Deno.test("should correctly parse booleans", () => {
  const input = `{true, fAlSe, TRUE1, TRUE, FALSE}`;
  const expected = [true, false, "TRUE1", true, false];
  const result = parsePgObject(input);
  assertEquals(result, expected);
});

// Tests for nulls
Deno.test("should correctly parse nulls", () => {
  const input = `{NULL, NULLA, ANULL, nULl, NULL}`;
  const expected = [null, "NULLA", "ANULL", null, null];
  const result = parsePgObject(input);
  assertEquals(result, expected);
});

// Tests for nested arrays
Deno.test("should correctly parse nested arrays", () => {
  const input = `{{"a", "b"}, {"c", "d"}}`;
  const expected = [["a", "b"], ["c", "d"]];
  const result = parsePgObject(input);
  assertEquals(result, expected);
});

// Tests for nested objects
Deno.test("should correctly parse nested objects", () => {
  const input = `(("a", "b"), ("c", "d"))`;
  const expected = { "0": { "0": "a", "1": "b" }, "1": { "0": "c", "1": "d" } };
  const result = parsePgObject(input);
  assertEquals(result, expected);
});

// Tests for complex cases
Deno.test("should correctly parse complex nested structures", () => {
  const input = `("foo()\\"{},", "bar", 1.23, {"string", 1, NULL, true}, ("nested()", NULL), true)`;
  const expected = {
    "0": 'foo()"{},',
    "1": "bar",
    "2": 1.23,
    "3": ["string", 1, null, true],
    "4": {
      "0": "nested()",
      "1": null,
    },
    "5": true,
  };
  const result = parsePgObject(input);
  assertEquals(result, expected);
});
