"""
policy_compiler.lexer — PolicyScript tokeniser.

INPUT:  source: str  (.policy file text)
OUTPUT: list[Token]

§48 boundary: stdlib only.
Zone 3 purity: no imports from any other project file.
"""
from __future__ import annotations
from dataclasses import dataclass

# ── Token types ────────────────────────────────────────────────────────────

# Case-sensitive keywords — exact set from spec + boolean/null literals.
# 'depends_on' is included: matches [A-Za-z_][A-Za-z0-9_]* so it is caught
# in the identifier scan and reclassified.
_KEYWORDS: frozenset[str] = frozenset({
    "service", "policy", "rule", "forum", "manager", "member",
    "permission", "intent", "proposal", "actors", "namespace",
    "import", "state", "when", "depends_on", "extends",
    "AND", "OR", "NOT", "IN",
    "true", "false", "null",
})

# Single-character symbol → token type
_SINGLE_CHAR: dict[str, str] = {
    "{": "LBRACE",
    "}": "RBRACE",
    "(": "LPAREN",
    ")": "RPAREN",
    "[": "LBRACKET",
    "]": "RBRACKET",
    ":": "COLON",
    ",": "COMMA",
    ".": "DOT",
}


# ── Data structures ────────────────────────────────────────────────────────

@dataclass
class Token:
    type:  str
    value: str
    line:  int
    col:   int

    def __repr__(self) -> str:
        return f"Token({self.type!r}, {self.value!r}, {self.line}:{self.col})"


class LexError(Exception):
    """Raised on any character the lexer cannot recognise."""

    def __init__(self, message: str, line: int, col: int) -> None:
        super().__init__(message)
        self.message = message
        self.line    = line
        self.col     = col

    def __str__(self) -> str:
        return f"{self.message} (line {self.line}, col {self.col})"


# ── Public API ─────────────────────────────────────────────────────────────

def tokenise(source: str) -> list[Token]:
    """
    Tokenise a PolicyScript source string.

    Returns a list of Token objects. NEWLINE, WHITESPACE, and COMMENT
    tokens are consumed and discarded — they do not appear in the output.

    Note on comment styles:
      - // ... (inline)  — spec-defined
      - /* ... */        — spec-defined block comment
      - # ...            — accepted as inline comment to handle .policy scaffold
                           files produced by policy_exporter.py, which uses # lines
                           for GOVERNANCE GAP and DIAGNOSTIC markers.

    Raises LexError for any unrecognised character.
    """
    tokens: list[Token] = []
    i      = 0
    line   = 1
    col    = 1
    n      = len(source)

    while i < n:
        c         = source[i]
        tok_line  = line
        tok_col   = col

        # ── Newline ───────────────────────────────────────────
        if c == "\n":
            line += 1
            col   = 1
            i    += 1
            continue

        # ── Whitespace (space, tab, carriage return) ──────────
        if c in " \t\r":
            col += 1
            i   += 1
            continue

        # ── Comments ──────────────────────────────────────────
        if c == "/" and i + 1 < n:
            if source[i + 1] == "/":
                # Inline comment: skip to end of line
                while i < n and source[i] != "\n":
                    i   += 1
                    col += 1
                continue
            if source[i + 1] == "*":
                # Block comment: skip until */
                i   += 2
                col += 2
                while i < n:
                    if source[i] == "\n":
                        line += 1
                        col   = 1
                        i    += 1
                    elif source[i] == "*" and i + 1 < n and source[i + 1] == "/":
                        i   += 2
                        col += 2
                        break
                    else:
                        col += 1
                        i   += 1
                continue

        # Hash comment — produced by policy_exporter.py scaffold output
        if c == "#":
            while i < n and source[i] != "\n":
                i   += 1
                col += 1
            continue

        # ── String literals ───────────────────────────────────
        if c == '"':
            i   += 1
            col += 1
            start = i
            while i < n and source[i] != '"':
                if source[i] == "\n":
                    raise LexError(
                        "Unterminated string literal", tok_line, tok_col
                    )
                i   += 1
                col += 1
            if i >= n:
                raise LexError(
                    "Unterminated string literal", tok_line, tok_col
                )
            value = source[start:i]
            i   += 1  # consume closing "
            col += 1
            tokens.append(Token("STRING", value, tok_line, tok_col))
            continue

        # ── Numeric literals ──────────────────────────────────
        if c.isdigit():
            start = i
            while i < n and source[i].isdigit():
                i   += 1
                col += 1
            # Optional decimal part
            if (i < n and source[i] == "."
                    and i + 1 < n and source[i + 1].isdigit()):
                i   += 1
                col += 1
                while i < n and source[i].isdigit():
                    i   += 1
                    col += 1
            tokens.append(Token("NUMBER", source[start:i], tok_line, tok_col))
            continue

        # ── Identifiers and keywords ──────────────────────────
        if c.isalpha() or c == "_":
            start = i
            while i < n and (source[i].isalnum() or source[i] == "_"):
                i   += 1
                col += 1
            value    = source[start:i]
            tok_type = "KEYWORD" if value in _KEYWORDS else "IDENTIFIER"
            tokens.append(Token(tok_type, value, tok_line, tok_col))
            continue

        # ── Two-character operators (must be checked before single-char) ──

        # Arrow: ->
        if c == "-":
            if i + 1 < n and source[i + 1] == ">":
                tokens.append(Token("ARROW", "->", tok_line, tok_col))
                i   += 2
                col += 2
            else:
                raise LexError(
                    f"Unexpected character '-' (did you mean '->'?)",
                    tok_line, tok_col,
                )
            continue

        # Equality: == or assignment =
        if c == "=":
            if i + 1 < n and source[i + 1] == "=":
                tokens.append(Token("EQ", "==", tok_line, tok_col))
                i   += 2
                col += 2
            else:
                tokens.append(Token("ASSIGN", "=", tok_line, tok_col))
                i   += 1
                col += 1
            continue

        # Not-equal: != (bare ! is illegal)
        if c == "!":
            if i + 1 < n and source[i + 1] == "=":
                tokens.append(Token("NEQ", "!=", tok_line, tok_col))
                i   += 2
                col += 2
            else:
                raise LexError(
                    "Unexpected character '!' (did you mean '!='?)",
                    tok_line, tok_col,
                )
            continue

        # Greater-than: >= or >
        if c == ">":
            if i + 1 < n and source[i + 1] == "=":
                tokens.append(Token("GTE", ">=", tok_line, tok_col))
                i   += 2
                col += 2
            else:
                tokens.append(Token("GT", ">", tok_line, tok_col))
                i   += 1
                col += 1
            continue

        # Less-than: <= or <
        if c == "<":
            if i + 1 < n and source[i + 1] == "=":
                tokens.append(Token("LTE", "<=", tok_line, tok_col))
                i   += 2
                col += 2
            else:
                tokens.append(Token("LT", "<", tok_line, tok_col))
                i   += 1
                col += 1
            continue

        # ── Single-character tokens ────────────────────────────
        if c in _SINGLE_CHAR:
            tokens.append(Token(_SINGLE_CHAR[c], c, tok_line, tok_col))
            i   += 1
            col += 1
            continue

        # ── Unrecognised character ─────────────────────────────
        raise LexError(
            f"Unexpected character {c!r}",
            tok_line, tok_col,
        )

    return tokens
