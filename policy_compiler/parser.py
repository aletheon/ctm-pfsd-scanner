"""
policy_compiler.parser — PolicyScript recursive-descent parser.

INPUT:  tokens: list[Token]  (from lexer.tokenise())
OUTPUT: dict  (AST — see structure below)

§48 boundary: stdlib only + lexer import.
Zone 3 purity: no imports from any other project file.

AST structure:
  {
    "namespace":  str | null,
    "imports":    [str],
    "actors":     { name: { "children": {name: ...} } },
    "states":     { name: { "fields": {name: str} } },
    "intents":    { name: { "fields": {name: str} } },
    "services":   { name: { "endpoint", "schema", "annotations" } },
    "policies":   { name: { "extends", "actor_scope", "state", "rules" } },
    "managers":   { name: { "permissions": [str] } },
    "members":    { name: { "permissions": [str] } },
  }
"""
from __future__ import annotations
from policy_compiler.lexer import Token


# ── Public error type ─────────────────────────────────────────────────────

class ParseError(Exception):
    """Raised on any structural violation of the PolicyScript grammar."""

    def __init__(self, message: str, line: int, col: int) -> None:
        super().__init__(message)
        self.message = message
        self.line    = line
        self.col     = col

    def __str__(self) -> str:
        return f"{self.message} (line {self.line}, col {self.col})"


# ── Public API ────────────────────────────────────────────────────────────

def parse(tokens: list[Token]) -> dict:
    """
    Parse a list of tokens produced by lexer.tokenise() into an AST dict.
    Raises ParseError on any grammar violation.
    """
    return _Parser(tokens).parse_program()


# ── Parser implementation ─────────────────────────────────────────────────

class _Parser:

    def __init__(self, tokens: list[Token]) -> None:
        self._tokens = tokens
        self._pos    = 0

    # ── Cursor helpers ─────────────────────────────────────────────────────

    def _peek(self, offset: int = 0) -> Token | None:
        idx = self._pos + offset
        return self._tokens[idx] if idx < len(self._tokens) else None

    def _advance(self) -> Token:
        tok = self._tokens[self._pos]
        self._pos += 1
        return tok

    def _at_end(self) -> bool:
        return self._pos >= len(self._tokens)

    def _current_pos(self) -> tuple[int, int]:
        tok = self._peek()
        if tok:
            return tok.line, tok.col
        if self._tokens:
            last = self._tokens[-1]
            return last.line, last.col
        return 1, 1

    def _expect(self, type_: str, value: str | None = None) -> Token:
        """Consume a token of the given type (and optionally value)."""
        tok = self._peek()
        if tok is None:
            line, col = self._current_pos()
            need = f"{type_!r}" + (f" {value!r}" if value else "")
            raise ParseError(
                f"E001 SyntaxError: expected {need}, got end of input",
                line, col,
            )
        if tok.type != type_:
            need = f"{type_!r}" + (f" {value!r}" if value else "")
            raise ParseError(
                f"E001 SyntaxError: expected {need}, got {tok.type!r} {tok.value!r}",
                tok.line, tok.col,
            )
        if value is not None and tok.value != value:
            raise ParseError(
                f"E001 SyntaxError: expected {value!r}, got {tok.value!r}",
                tok.line, tok.col,
            )
        return self._advance()

    def _expect_name(self) -> Token:
        """
        Consume the current token as a name.
        Accepts IDENTIFIER or KEYWORD — field names and type names can clash
        with reserved words in certain positions (e.g. 'state' as a type).
        """
        tok = self._peek()
        if tok is None:
            line, col = self._current_pos()
            raise ParseError(
                "E001 SyntaxError: expected identifier, got end of input",
                line, col,
            )
        if tok.type not in ("IDENTIFIER", "KEYWORD"):
            raise ParseError(
                f"E001 SyntaxError: expected identifier, got {tok.type!r} {tok.value!r}",
                tok.line, tok.col,
            )
        return self._advance()

    def _is_keyword(self, *values: str) -> bool:
        tok = self._peek()
        return tok is not None and tok.type == "KEYWORD" and tok.value in values

    def _is_type(self, type_: str) -> bool:
        tok = self._peek()
        return tok is not None and tok.type == type_

    # ── Top-level program ──────────────────────────────────────────────────

    def parse_program(self) -> dict:
        ast: dict = {
            "namespace": None,
            "imports":   [],
            "actors":    {},
            "states":    {},
            "intents":   {},
            "services":  {},
            "policies":  {},
            "managers":  {},
            "members":   {},
        }

        while not self._at_end():
            tok = self._peek()
            if tok is None:
                break

            if tok.type != "KEYWORD":
                raise ParseError(
                    f"E001 SyntaxError: unexpected token {tok.type!r} "
                    f"{tok.value!r} at top level",
                    tok.line, tok.col,
                )

            kw = tok.value
            self._advance()

            if kw == "namespace":
                ast["namespace"] = self._parse_dotted_name()

            elif kw == "import":
                ast["imports"].append(self._expect("STRING").value)

            elif kw == "actors":
                self._expect("LBRACE")
                ast["actors"] = self._parse_actor_children()
                self._expect("RBRACE")

            elif kw == "state":
                name, body = self._parse_field_block()
                ast["states"][name] = body

            elif kw == "intent":
                name, body = self._parse_field_block()
                ast["intents"][name] = body

            elif kw == "service":
                name, svc = self._parse_service()
                ast["services"][name] = svc

            elif kw == "policy":
                name, pol = self._parse_policy()
                ast["policies"][name] = pol

            elif kw == "manager":
                name, mgr = self._parse_permission_holder()
                ast["managers"][name] = mgr

            elif kw == "member":
                name, mem = self._parse_permission_holder()
                ast["members"][name] = mem

            else:
                raise ParseError(
                    f"E001 SyntaxError: unexpected keyword {kw!r} at top level",
                    tok.line, tok.col,
                )

        return ast

    # ── Dotted name ────────────────────────────────────────────────────────

    def _parse_dotted_name(self) -> str:
        """Parse IDENTIFIER ('.' IDENTIFIER)* — used for namespace."""
        tok = self._expect_name()
        parts = [tok.value]
        while self._is_type("DOT"):
            self._advance()          # consume .
            parts.append(self._expect_name().value)
        return ".".join(parts)

    # ── Actor hierarchy (recursive nested dict) ───────────────────────────

    def _parse_actor_children(self) -> dict:
        """
        Parse the interior of an actors { } block recursively.
        Each child is: IDENTIFIER [ '{' <actor_children> '}' ]
        Returns { name: { "children": {...} } }
        """
        children: dict = {}
        while not self._at_end() and not self._is_type("RBRACE"):
            name_tok = self._expect_name()
            name = name_tok.value
            if self._is_type("LBRACE"):
                self._advance()
                sub = self._parse_actor_children()
                self._expect("RBRACE")
                children[name] = {"children": sub}
            else:
                children[name] = {"children": {}}
        return children

    # ── State / intent field block ─────────────────────────────────────────

    def _parse_field_block(self) -> tuple[str, dict]:
        """
        Parse: IDENTIFIER '{' (IDENTIFIER ':' IDENTIFIER)* '}'
        Used for state and intent declarations.
        """
        name = self._expect_name().value
        self._expect("LBRACE")
        fields: dict = {}
        while not self._at_end() and not self._is_type("RBRACE"):
            field_name = self._expect_name().value
            self._expect("COLON")
            field_type = self._expect_name().value
            fields[field_name] = field_type
            if self._is_type("COMMA"):
                self._advance()
        self._expect("RBRACE")
        return name, {"fields": fields}

    # ── Service declaration ────────────────────────────────────────────────

    def _parse_service(self) -> tuple[str, dict]:
        """
        Parse a service block:
          IDENTIFIER '{' endpoint? schema? simulation_safe? ... '}'
        """
        name = self._expect_name().value
        self._expect("LBRACE")

        svc: dict = {
            "endpoint": None,
            "schema":   {},
            "annotations": {
                "simulation_safe":       None,
                "health_monitor":        None,
                "allow_nondeterminism":  False,
                "failure_threshold":     None,
            },
        }

        while not self._at_end() and not self._is_type("RBRACE"):
            tok = self._peek()
            if tok is None:
                break
            if tok.type not in ("IDENTIFIER", "KEYWORD"):
                raise ParseError(
                    f"E001 SyntaxError: unexpected token in service body: "
                    f"{tok.type!r} {tok.value!r}",
                    tok.line, tok.col,
                )
            key = tok.value
            self._advance()

            if key == "endpoint":
                self._expect("COLON")
                svc["endpoint"] = self._expect("STRING").value

            elif key == "schema":
                self._expect("LBRACE")
                svc["schema"] = self._parse_schema_methods()
                self._expect("RBRACE")

            elif key == "simulation_safe":
                self._expect("COLON")
                svc["annotations"]["simulation_safe"] = self._parse_bool()

            elif key == "health_monitor":
                self._expect("COLON")
                svc["annotations"]["health_monitor"] = self._parse_bool()

            elif key == "allow_nondeterminism":
                self._expect("COLON")
                svc["annotations"]["allow_nondeterminism"] = self._parse_bool()

            elif key == "failure_threshold":
                self._expect("COLON")
                n = self._expect("NUMBER")
                svc["annotations"]["failure_threshold"] = int(float(n.value))

            else:
                raise ParseError(
                    f"E001 SyntaxError: unknown service property {key!r}",
                    tok.line, tok.col,
                )

        self._expect("RBRACE")
        return name, svc

    def _parse_schema_methods(self) -> dict:
        """
        Parse method declarations inside a service schema block.
        Each method: IDENTIFIER '(' param_list ')' '->' return_type
        """
        methods: dict = {}
        while not self._at_end() and not self._is_type("RBRACE"):
            method_name = self._expect_name().value
            self._expect("LPAREN")

            params: list = []
            while not self._at_end() and not self._is_type("RPAREN"):
                param_name = self._expect_name().value
                self._expect("COLON")
                param_type = self._expect_name().value
                params.append({"name": param_name, "type": param_type})
                if self._is_type("COMMA"):
                    self._advance()

            self._expect("RPAREN")
            self._expect("ARROW")
            return_type = self._parse_schema_return_type()

            methods[method_name] = {"params": params, "returns": return_type}

        return methods

    def _parse_schema_return_type(self) -> str:
        """
        Parse the return type after '->'. Handles simple types ('float')
        and compound kinds ('state_delta ModuleState').

        Consumes one or more IDENTIFIER/KEYWORD tokens, stopping when:
          - RBRACE (end of schema block), or
          - An IDENTIFIER followed immediately by LPAREN (next method name).
        """
        tok = self._peek()
        if tok is None or tok.type not in ("IDENTIFIER", "KEYWORD"):
            line, col = self._current_pos()
            raise ParseError(
                "E001 SyntaxError: expected return type after '->'",
                line, col,
            )

        parts = [self._advance().value]  # consume the required first part

        # Greedily consume additional parts (e.g. "state_delta ModuleState")
        while not self._at_end():
            cur  = self._peek()
            nxt  = self._peek(1)
            if cur is None or cur.type == "RBRACE":
                break
            if cur.type not in ("IDENTIFIER", "KEYWORD"):
                break
            # Stop if this identifier looks like the start of the next method
            if nxt is not None and nxt.type == "LPAREN":
                break
            parts.append(self._advance().value)

        return " ".join(parts)

    # ── Policy declaration ─────────────────────────────────────────────────

    def _parse_policy(self) -> tuple[str, dict]:
        """
        Parse a policy block:
          IDENTIFIER [ 'extends' IDENTIFIER ] '{' body '}'

        Detects:
          E-FCL-001 — 'forum' keyword inside policy body
          E-FCL-002 — 'member' keyword inside policy body
        """
        name    = self._expect_name().value
        extends = None

        if self._is_keyword("extends"):
            self._advance()
            extends = self._expect_name().value

        self._expect("LBRACE")

        pol: dict = {
            "extends":     extends,
            "actor_scope": [],
            "state":       None,
            "rules":       [],
        }

        while not self._at_end() and not self._is_type("RBRACE"):
            tok = self._peek()
            if tok is None:
                break

            # ── FCL guards ────────────────────────────────────────────────
            if tok.type == "KEYWORD" and tok.value == "forum":
                raise ParseError(
                    "E-FCL-001: inline forum declaration inside policy body "
                    "is not permitted",
                    tok.line, tok.col,
                )
            if tok.type == "KEYWORD" and tok.value == "member":
                raise ParseError(
                    "E-FCL-002: inline member assignment inside policy body "
                    "is not permitted",
                    tok.line, tok.col,
                )

            # ── actor_scope: [ name, ... ] ────────────────────────────────
            if tok.type == "IDENTIFIER" and tok.value == "actor_scope":
                self._advance()
                self._expect("COLON")
                pol["actor_scope"] = self._parse_name_list_brackets()
                continue

            # ── state reference: state IDENTIFIER ─────────────────────────
            if tok.type == "KEYWORD" and tok.value == "state":
                self._advance()
                pol["state"] = self._expect_name().value
                continue

            # ── rule declaration ───────────────────────────────────────────
            if tok.type == "KEYWORD" and tok.value == "rule":
                self._advance()
                pol["rules"].append(self._parse_rule())
                continue

            raise ParseError(
                f"E001 SyntaxError: unexpected token in policy body: "
                f"{tok.type!r} {tok.value!r}",
                tok.line, tok.col,
            )

        self._expect("RBRACE")
        return name, pol

    # ── Rule declaration ───────────────────────────────────────────────────

    def _parse_rule(self) -> dict:
        """
        Parse a rule inside a policy body:
          IDENTIFIER
          [ 'when' <condition_text> ]
          [ 'depends_on' IDENTIFIER ]
          '->' IDENTIFIER [ '.' IDENTIFIER '(' arg_list ')' ]
        """
        name       = self._expect_name().value
        when       = None
        depends_on = None

        # Optional: when <condition>
        if self._is_keyword("when"):
            self._advance()
            raw = self._collect_condition_tokens()
            when = raw if raw.strip() else None

        # Optional: depends_on IDENTIFIER
        if self._is_keyword("depends_on"):
            self._advance()
            depends_on = self._expect_name().value

        # Required: -> target
        self._expect("ARROW")
        target_base = self._expect_name().value

        target = target_base
        params: dict | None = None

        if self._is_type("DOT"):
            self._advance()
            method_name = self._expect_name().value
            target = f"{target_base}.{method_name}"
            if self._is_type("LPAREN"):
                self._advance()
                params = self._parse_arg_list()
                self._expect("RPAREN")

        return {
            "name":       name,
            "when":       when,
            "depends_on": depends_on,
            "target":     target,
            "params":     params,
        }

    def _collect_condition_tokens(self) -> str:
        """
        Collect tokens as raw condition text until a rule-structural token:
          - KEYWORD 'depends_on'   (rule directive)
          - ARROW                  (rule target separator)
          - KEYWORD 'rule'         (start of next rule)
          - RBRACE                 (end of policy body)

        These terminators cannot appear in a valid condition expression.
        Tokens are joined by single spaces. Callers trim the result.
        """
        parts: list[str] = []
        while not self._at_end():
            tok = self._peek()
            if tok is None:
                break
            if tok.type == "RBRACE":
                break
            if tok.type == "ARROW":
                break
            if tok.type == "KEYWORD" and tok.value in ("depends_on", "rule"):
                break
            parts.append(self._advance().value)
        return " ".join(parts)

    def _parse_arg_list(self) -> dict:
        """
        Parse named arguments: (IDENTIFIER ':' value)*
        Stops at RPAREN.
        """
        args: dict = {}
        while not self._at_end() and not self._is_type("RPAREN"):
            arg_name = self._expect_name().value
            self._expect("COLON")
            args[arg_name] = self._parse_arg_value()
            if self._is_type("COMMA"):
                self._advance()
        return args

    def _parse_arg_value(self):
        """Parse a rule-invocation argument value: NUMBER | STRING | BOOLEAN | IDENTIFIER."""
        tok = self._peek()
        if tok is None:
            line, col = self._current_pos()
            raise ParseError(
                "E001 SyntaxError: expected argument value, got end of input",
                line, col,
            )
        if tok.type == "NUMBER":
            self._advance()
            return float(tok.value) if "." in tok.value else int(tok.value)
        if tok.type == "STRING":
            self._advance()
            return tok.value
        if tok.type == "KEYWORD":
            if tok.value == "true":
                self._advance()
                return True
            if tok.value == "false":
                self._advance()
                return False
            if tok.value == "null":
                self._advance()
                return None
        if tok.type == "IDENTIFIER":
            self._advance()
            return tok.value
        raise ParseError(
            f"E001 SyntaxError: expected argument value, "
            f"got {tok.type!r} {tok.value!r}",
            tok.line, tok.col,
        )

    # ── Manager / member declaration ───────────────────────────────────────

    def _parse_permission_holder(self) -> tuple[str, dict]:
        """
        Parse: IDENTIFIER '{' 'permissions' '{' name_list '}' '}'
        Used for both manager and member declarations.
        """
        name = self._expect_name().value
        self._expect("LBRACE")

        perms: list[str] = []

        while not self._at_end() and not self._is_type("RBRACE"):
            tok = self._peek()
            if tok is None:
                break
            if tok.type in ("IDENTIFIER", "KEYWORD") and tok.value == "permissions":
                self._advance()
                self._expect("LBRACE")
                while not self._at_end() and not self._is_type("RBRACE"):
                    perms.append(self._expect_name().value)
                    if self._is_type("COMMA"):
                        self._advance()
                self._expect("RBRACE")
            else:
                raise ParseError(
                    f"E001 SyntaxError: unexpected token in manager/member body: "
                    f"{tok.type!r} {tok.value!r}",
                    tok.line, tok.col,
                )

        self._expect("RBRACE")
        return name, {"permissions": perms}

    # ── Name list ──────────────────────────────────────────────────────────

    def _parse_name_list_brackets(self) -> list[str]:
        """Parse '[' (IDENTIFIER (',' IDENTIFIER)*)? ']'."""
        self._expect("LBRACKET")
        names: list[str] = []
        while not self._at_end() and not self._is_type("RBRACKET"):
            names.append(self._expect_name().value)
            if self._is_type("COMMA"):
                self._advance()
        self._expect("RBRACKET")
        return names

    # ── Boolean literal ────────────────────────────────────────────────────

    def _parse_bool(self) -> bool:
        tok = self._peek()
        if tok and tok.type == "KEYWORD" and tok.value == "true":
            self._advance()
            return True
        if tok and tok.type == "KEYWORD" and tok.value == "false":
            self._advance()
            return False
        line, col = self._current_pos()
        got = f"{tok.type!r} {tok.value!r}" if tok else "end of input"
        raise ParseError(
            f"E001 SyntaxError: expected boolean (true/false), got {got}",
            line, col,
        )
