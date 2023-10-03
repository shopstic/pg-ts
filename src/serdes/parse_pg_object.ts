function parseNumber(str: string, index: number): [number, number] {
  let value = "";
  let isFloat = false;
  let inExponent = false;

  if (str[index] === "-") {
    value += "-";
    index++;
  }

  while (true) {
    const charCode = str.charCodeAt(index);
    const char = str[index];

    // ASCII code range for digits is 48-57
    if (
      (charCode >= 48 && charCode <= 57) || char === "." || (inExponent && (char === "-" || char === "+")) ||
      char.toLowerCase() === "e"
    ) {
      if (char === ".") {
        if (isFloat || inExponent) {
          throw new Error(`Invalid float number, so far got ${value}`);
        }
        isFloat = true;
      } else if (char.toLowerCase() === "e") {
        if (inExponent) {
          throw new Error(`Invalid float number with multiple 'e', so far got ${value}`);
        }
        isFloat = true;
        inExponent = true;
      }

      value += str[index];
      index++;
    } else {
      break;
    }
  }

  return [Number(value), index];
}

function parseUnquotedString(str: string, index: number): [string, number] {
  let result = "";

  while (index < str.length && str[index] !== "," && str[index] !== " " && str[index] !== ")" && str[index] !== "}") {
    result += str[index];
    index++;
  }

  return [result, index];
}

function parseString(str: string, index: number): [string, number] {
  if (str[index] !== '"') {
    return parseUnquotedString(str, index);
  }

  index++; // Skip opening quote
  const startIndex = index;
  let hasEscapes = false;
  let prevCharWasEscape = false;

  while (index < str.length) {
    if (str[index] === "\\" && !prevCharWasEscape) {
      hasEscapes = true;
      prevCharWasEscape = true;
    } else {
      if (str[index] === '"' && !prevCharWasEscape) {
        break;
      }
      prevCharWasEscape = false;
    }
    index++;
  }

  if (str[index] !== '"') {
    throw new Error(`Unterminated string, so far got ${str.slice(startIndex, index)}`);
  }

  const parsedString = str.slice(startIndex, index);
  return [hasEscapes ? parsedString.replace(/\\"/g, '"') : parsedString, index + 1]; // Skip closing quote
}

function isBoundaryChar(ch: string): boolean {
  return ch === "," || ch === " " || ch === ")" || ch === "}" || ch === undefined;
}

function startsWithCaseInsensitiveAscii(str: string, startIndex: number, literal: string): boolean {
  for (let i = 0; i < literal.length; i++) {
    if ((str.charCodeAt(startIndex + i) | 0x20) !== (literal.charCodeAt(i) | 0x20)) {
      return false;
    }
  }
  return true;
}

const spaceRegex = /\s/;

function parseValue(str: string, index: number): [unknown, number] {
  // Skip leading whitespaces
  while (spaceRegex.test(str[index]) && index < str.length) {
    index++;
  }

  const currentChar = str[index];

  if (currentChar === '"') {
    return parseString(str, index);
  }

  if (!isNaN(Number(currentChar)) || currentChar === "." || (currentChar === "-" && !isNaN(Number(str[index + 1])))) {
    return parseNumber(str, index);
  }

  if (startsWithCaseInsensitiveAscii(str, index, "true") && isBoundaryChar(str[index + 4])) {
    return [true, index + 4];
  } else if (startsWithCaseInsensitiveAscii(str, index, "false") && isBoundaryChar(str[index + 5])) {
    return [false, index + 5];
  } else if (startsWithCaseInsensitiveAscii(str, index, "null") && isBoundaryChar(str[index + 4])) {
    return [null, index + 4];
  } else if (currentChar === "{") {
    return parseCollection(str, index, "}", []);
  } else if (currentChar === "(") {
    return parseCollection(str, index, ")", {});
  }

  // Fall back to parsing as an unquoted string
  return parseUnquotedString(str, index);
}

const collectionItemBoundaryRegex = /[\s,]/;

function parseCollection(
  str: string,
  index: number,
  endToken: string,
  holder: unknown[] | { [key: string]: unknown },
): [unknown, number] {
  index++; // Skip the opening token
  let fieldIndex = 0;
  const isArray = Array.isArray(holder);

  while (index < str.length) {
    let value: unknown;

    if (str[index] === endToken) {
      break;
    }

    [value, index] = parseValue(str, index);

    if (isArray) {
      holder.push(value);
    } else {
      holder[fieldIndex] = value;
      fieldIndex++;
    }

    // Skip until the next item's boundary
    while (collectionItemBoundaryRegex.test(str[index]) && index < str.length) {
      index++;
    }
  }

  if (str[index] !== endToken) {
    throw new Error(`Expected end token ${endToken}, found ${str[index]}`);
  }

  return [holder, index + 1];
}

export function parsePgObject(str: string): unknown {
  let value: unknown;
  let index = 0;

  [value, index] = parseValue(str, 0);

  if (index < str.length) {
    throw new Error(
      `Unexpected remaining characters found after finished parsing at index ${index}. Here's the remaining part sliced up to 100 chars: ${
        str.slice(index, 100)
      }`,
    );
  }

  return value;
}
