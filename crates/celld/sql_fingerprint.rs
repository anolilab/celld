// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The fingerprint of an SQL statement: a hash of its shape that no literal
//! can reach. See [`fingerprint`].

/// A hash of `sql` as a shape. Whitespace and comments collapse, keywords and
/// names fold to lower case, and every literal becomes one placeholder: a
/// string, a blob, a number (with its sign when the sign is unary, as in
/// `a = -1`), `NULL`, `TRUE`, `FALSE` (except after `IS` or `IS NOT`, where
/// they are part of the shape), a parameter, and any quoted token (`"..."`,
/// `` `...` `` and `[...]`). A double-quoted token that names no column is a
/// string to SQLite, so it can hold user data like a single-quoted one; the
/// fingerprint cannot tell which it is, and treats all of them as literals.
/// A named parameter (`:name`, `@name`, `$name`) is a placeholder like `?`.
/// A list after `IN` of only placeholders is one placeholder. Two statements
/// that differ only in those get the same value. Nothing is allocated.
///
/// The fingerprint is of the first statement only: it ends at the first `;`
/// outside a quote or comment. A caller that hands over the rest of a script
/// after a statement that failed to prepare gets the failing statement's shape,
/// whatever follows it. (A `CREATE TRIGGER` body holds `;`, so a trigger's
/// shape is the part up to its first one.)
pub(crate) fn fingerprint(sql: &str) -> u64 {
    let bytes = sql.as_bytes();
    let word = |byte: u8| byte.is_ascii_alphanumeric() || byte == b'_' || byte >= 0x80;
    let mut shape = Shape::default();
    let mut at = 0;
    while at < bytes.len() {
        let byte = bytes[at];
        if byte.is_ascii_whitespace() {
            at += 1;
        } else if byte == b'-' && bytes.get(at + 1) == Some(&b'-') {
            while at < bytes.len() && bytes[at] != b'\n' {
                at += 1;
            }
        } else if byte == b'/' && bytes.get(at + 1) == Some(&b'*') {
            at += 2;
            while at < bytes.len() && !(bytes[at] == b'*' && bytes.get(at + 1) == Some(&b'/')) {
                at += 1;
            }
            at = (at + 2).min(bytes.len());
        } else if byte == b'\'' || byte == b'"' || byte == b'`' {
            // A quote is doubled to put one inside.
            at += 1;
            while at < bytes.len() {
                if bytes[at] == byte {
                    if bytes.get(at + 1) == Some(&byte) {
                        at += 2;
                        continue;
                    }
                    at += 1;
                    break;
                }
                at += 1;
            }
            shape.placeholder();
        } else if byte == b'[' {
            while at < bytes.len() && bytes[at] != b']' {
                at += 1;
            }
            at = (at + 1).min(bytes.len());
            shape.placeholder();
        } else if byte == b'?' {
            // A parameter, with its number if it has one.
            at += 1;
            while at < bytes.len() && bytes[at].is_ascii_digit() {
                at += 1;
            }
            shape.placeholder();
        } else if matches!(byte, b':' | b'@' | b'$') && bytes.get(at + 1).is_some_and(|b| word(*b))
        {
            // A named parameter.
            at += 1;
            while at < bytes.len() && word(bytes[at]) {
                at += 1;
            }
            shape.placeholder();
        } else if byte == b';' {
            break;
        } else if byte.is_ascii_digit()
            || (byte == b'.' && bytes.get(at + 1).is_some_and(u8::is_ascii_digit))
            || (matches!(byte, b'+' | b'-')
                && !shape.operand
                && bytes.get(at + 1).is_some_and(|next| {
                    next.is_ascii_digit()
                        || (*next == b'.' && bytes.get(at + 2).is_some_and(u8::is_ascii_digit))
                }))
        {
            // Digits, a fraction, an exponent or a hex body. A sign where an
            // operand is expected (`a = -1`) belongs to the number; after one
            // (`a - 1`) it is an operator. The sign of an exponent belongs to
            // the number, but `0xe-1` is a subtraction.
            if matches!(byte, b'+' | b'-') {
                at += 1;
            }
            let hex = bytes[at] == b'0' && matches!(bytes.get(at + 1), Some(b'x' | b'X'));
            at += 1;
            while at < bytes.len() {
                let next = bytes[at];
                let signed_exponent = matches!(next, b'+' | b'-')
                    && !hex
                    && matches!(bytes[at - 1], b'e' | b'E')
                    && bytes.get(at + 1).is_some_and(u8::is_ascii_digit);
                if !(word(next) || next == b'.' || signed_exponent) {
                    break;
                }
                at += 1;
            }
            shape.placeholder();
        } else if matches!(byte, b'x' | b'X') && bytes.get(at + 1) == Some(&b'\'') {
            // A blob literal: the quote makes it a literal.
            at += 2;
            while at < bytes.len() && bytes[at] != b'\'' {
                at += 1;
            }
            at = (at + 1).min(bytes.len());
            shape.placeholder();
        } else if word(byte) {
            let start = at;
            while at < bytes.len() && word(bytes[at]) {
                at += 1;
            }
            shape.word(&bytes[start..at]);
        } else {
            at += 1;
            shape.punctuation(byte);
        }
    }
    shape.finish()
}

/// The hash so far, and what the fingerprint needs to know of the tokens
/// before the next one.
#[derive(Default)]
struct Shape {
    hash: Hash,
    /// The last token was a word or a placeholder: two of them in a row stay
    /// apart by a space, whether or not the source had one.
    atom: bool,
    /// The last token was the word `in`.
    after_in: bool,
    /// Parenthesis depth inside the list after `in`; 0 outside one.
    list_depth: u32,
    /// The last token in the list was a placeholder.
    list_placeholder: bool,
    /// The last token ended an operand: a name, a literal, a placeholder or
    /// `)`. A `+` or `-` after one is binary; after anything else it is a sign.
    operand: bool,
    /// How far the last words were `IS` (1) or `IS NOT` (2): a `NULL` there
    /// is part of the shape.
    after_is: u8,
    /// A comma that followed one, not yet hashed: another placeholder drops
    /// it, and anything else brings it back.
    held_comma: bool,
}

/// Keywords after which a `+` or `-` is a sign, not an operator: they are
/// followed by an operand.
const OPERATOR_KEYWORDS: &[&str] = &[
    "all",
    "and",
    "as",
    "between",
    "by",
    "case",
    "default",
    "distinct",
    "else",
    "escape",
    "glob",
    "having",
    "in",
    "is",
    "like",
    "limit",
    "match",
    "not",
    "offset",
    "on",
    "or",
    "regexp",
    "returning",
    "select",
    "set",
    "then",
    "values",
    "when",
    "where",
];

struct Hash(u64);

impl Default for Hash {
    fn default() -> Self {
        Self(0xcbf2_9ce4_8422_2325)
    }
}

impl Hash {
    fn feed(&mut self, byte: u8) {
        self.0 ^= u64::from(byte);
        self.0 = self.0.wrapping_mul(0x0100_0000_01b3);
    }
}

impl Shape {
    /// Hash the comma a list placeholder was holding back.
    fn release(&mut self) {
        if std::mem::take(&mut self.held_comma) {
            self.hash.feed(b',');
        }
    }

    /// Start a token. `atom` says it is a word or a placeholder.
    fn begin(&mut self, atom: bool) {
        self.release();
        if self.atom && atom {
            self.hash.feed(b' ');
        }
        self.atom = atom;
        self.after_in = false;
        self.after_is = 0;
    }

    fn placeholder(&mut self) {
        if self.held_comma {
            // `?, ?` in a list: the run is one placeholder.
            self.held_comma = false;
            return;
        }
        self.begin(true);
        self.hash.feed(b'?');
        self.operand = true;
        self.list_placeholder = self.list_depth == 1;
    }

    fn word(&mut self, word: &[u8]) {
        let was_is = self.after_is;
        let is = |name: &[u8]| word.eq_ignore_ascii_case(name);
        if was_is == 0 && (is(b"null") || is(b"true") || is(b"false")) {
            self.placeholder();
            self.after_is = 0;
            return;
        }
        self.begin(true);
        self.after_is = match (was_is, is(b"is"), is(b"not")) {
            (_, true, _) => 1,
            (1, _, true) => 2,
            _ => 0,
        };
        self.operand = !OPERATOR_KEYWORDS.iter().any(|name| is(name.as_bytes()));
        self.list_placeholder = false;
        for byte in word {
            self.hash.feed(byte.to_ascii_lowercase());
        }
        self.after_in = word.eq_ignore_ascii_case(b"in");
    }

    fn punctuation(&mut self, byte: u8) {
        let after_in = self.after_in;
        let in_list_run = byte == b',' && self.list_depth == 1 && self.list_placeholder;
        if in_list_run {
            self.operand = false;
            self.after_is = 0;
            self.held_comma = true;
            return;
        }
        self.begin(false);
        self.operand = byte == b')';
        self.list_placeholder = false;
        self.hash.feed(byte);
        match byte {
            b'(' if after_in && self.list_depth == 0 => self.list_depth = 1,
            b'(' if self.list_depth > 0 => self.list_depth += 1,
            b')' => self.list_depth = self.list_depth.saturating_sub(1),
            _ => {}
        }
    }

    fn finish(mut self) -> u64 {
        self.release();
        self.hash.0
    }
}

#[cfg(test)]
mod tests {
    use super::fingerprint;

    /// Statements in one group differ only in what the fingerprint drops, so
    /// every one of them has the group's first value.
    #[test]
    fn statements_of_one_shape_share_a_fingerprint() {
        let groups: &[&[&str]] = &[
            // Whitespace, comments, case, and string, number and blob literals.
            &[
                "SELECT * FROM t WHERE a = 1 AND b = 'x'",
                "select  *\n  from t -- note\n where a = 99999 and b = 'it''s'",
                "SELECT * FROM t /* c */ WHERE a = 2.5e3 AND b = x'00ff'",
                "SELECT * FROM t WHERE a = .5 AND b = X'AA'",
            ],
            // SQLite reads a double-quoted token that names no column as a
            // string, so it is user data like any other literal.
            &[
                r#"SELECT * FROM t WHERE a = "secret""#,
                r#"SELECT * FROM t WHERE a = "other""#,
                r#"SELECT * FROM t WHERE a = "it""s""#,
                "SELECT * FROM t WHERE a = 'x'",
            ],
            &[
                r#"SELECT * FROM t WHERE a IN ("x", "y")"#,
                "SELECT * FROM t WHERE a IN (1)",
            ],
            &[
                "SELECT `a` FROM [b] WHERE c = `d`",
                "SELECT `z` FROM [y x] WHERE c = `w``v`",
            ],
            // Parameters, numbered or named, are placeholders too.
            &[
                "SELECT * FROM t WHERE a = ? AND b = ?2",
                "SELECT * FROM t WHERE a = :a AND b = @b",
                "SELECT * FROM t WHERE a = $first AND b = :other_name",
                "SELECT * FROM t WHERE a = 1 AND b = 'x'",
            ],
            // A statement ends at its first top-level `;`: what follows it,
            // and a `;` inside a quote or comment, are not part of it.
            &[
                "SELECT * FROM missing WHERE a = 7",
                "SELECT * FROM missing WHERE a = 7;",
                "SELECT * FROM missing WHERE a = 7; SELECT 1",
                "SELECT * FROM missing WHERE a = 7 ; DROP TABLE other; SELECT 'x'",
            ],
            &["SELECT ';' /* ; */ -- ;\n, 1; SELECT 2", "SELECT 'a;b', 2"],
            // Unterminated input is read to its end, never past it.
            &["SELECT 'abc", "SELECT 'xyz"],
            &[r#"SELECT "abc"#, r#"SELECT "xyz"#],
            &["SELECT x'00", "SELECT x'ff"],
            &["SELECT 1 /* open", "SELECT 1 /* other"],
            &["SELECT 1 -- note", "SELECT 1"],
            // A literal right after a word has the same shape as one after a space.
            &["SELECT'x'", "SELECT 'y'", "SELECT ?", "SELECT/**/1"],
            &["SELECT (1", "SELECT ( 1"],
            // A sign after `DEFAULT` belongs to the number.
            &[
                "CREATE TABLE t (a DEFAULT -1)",
                "CREATE TABLE t (a DEFAULT 5)",
            ],
        ];
        assert_one_shape(groups);
    }

    /// An `IN` list of placeholders is one placeholder, and the sign of an
    /// exponent is part of its number.
    #[test]
    fn an_in_list_is_one_placeholder_and_an_exponent_keeps_its_sign() {
        assert_one_shape(&[
            &[
                "SELECT * FROM t WHERE a IN (1, 2, 3)",
                "SELECT * FROM t WHERE a IN (4)",
                "SELECT * FROM t WHERE a IN ('a','b')",
                "SELECT * FROM t WHERE a IN (?, ?, ?)",
                "SELECT * FROM t WHERE a in ( ?1 ,?2 )",
            ],
            &[
                "SELECT * FROM t WHERE a = 1e-5",
                "SELECT * FROM t WHERE a = 2.5E+10",
                "SELECT * FROM t WHERE a = .5e-3",
                "SELECT * FROM t WHERE a = 3",
            ],
        ]);
    }

    /// A sign that is unary belongs to its number; a binary one does not.
    #[test]
    fn a_unary_sign_is_part_of_its_literal() {
        assert_one_shape(&[
            &[
                "SELECT * FROM t WHERE a = 1",
                "SELECT * FROM t WHERE a = -1",
                "SELECT * FROM t WHERE a = +2.5",
                "SELECT * FROM t WHERE a = ?",
            ],
            &[
                "SELECT * FROM t WHERE a > -.5 AND b < -3",
                "SELECT * FROM t WHERE a > 1 AND b < 2",
            ],
            &[
                "SELECT * FROM t WHERE a IN (1, 2)",
                "SELECT * FROM t WHERE a IN (1, -2)",
                "SELECT * FROM t WHERE a IN (-1, -2, -3)",
            ],
            &["SELECT * FROM t LIMIT 1", "SELECT * FROM t LIMIT -1"],
            &["SELECT 1", "SELECT -1"],
        ]);
        // After an operand the sign is an operator, so a subtraction keeps
        // its minus and differs from the bare operand.
        assert_one_shape(&[
            &["SELECT a - 1", "SELECT a -1", "SELECT a - 7"],
            &["SELECT 1 - 1", "SELECT 2 -3", "SELECT ? - ?"],
            &["SELECT (a) - 1", "SELECT (a) -2"],
        ]);
        for (subtraction, bare) in [
            ("SELECT a - 1", "SELECT a 1"),
            ("SELECT 1 - 1", "SELECT 1"),
            ("SELECT (a) - 1", "SELECT (a) 1"),
        ] {
            assert_ne!(fingerprint(subtraction), fingerprint(bare), "{subtraction}");
        }
    }

    /// `NULL`, `TRUE` and `FALSE` are literals, except after `IS` and `IS NOT`.
    #[test]
    fn null_and_booleans_are_literals_except_after_is() {
        assert_one_shape(&[
            &[
                "SELECT * FROM t WHERE a = NULL",
                "SELECT * FROM t WHERE a = 1",
                "SELECT * FROM t WHERE a = true",
                "SELECT * FROM t WHERE a = FALSE",
            ],
            &[
                "INSERT INTO t VALUES (null, 1)",
                "INSERT INTO t VALUES (1, 2)",
            ],
            &[
                "SELECT * FROM t WHERE a IS NULL",
                "SELECT * FROM t WHERE a is null",
            ],
        ]);
        let shapes = [
            "SELECT * FROM t WHERE a IS NULL",
            "SELECT * FROM t WHERE a IS NOT NULL",
            "SELECT * FROM t WHERE a = ?",
            "SELECT * FROM t WHERE a IS NOT ?",
        ];
        let values: Vec<u64> = shapes.iter().map(|sql| fingerprint(sql)).collect();
        for (index, value) in values.iter().enumerate() {
            assert!(!values[..index].contains(value), "{}", shapes[index]);
        }
    }

    fn assert_one_shape(groups: &[&[&str]]) {
        for group in groups {
            let first = fingerprint(group[0]);
            for sql in *group {
                assert_eq!(fingerprint(sql), first, "{sql}");
            }
        }
    }

    /// Statements in one group differ in a way that matters, so no two of
    /// them share a fingerprint.
    #[test]
    fn statements_of_different_shapes_differ() {
        let groups: &[&[&str]] = &[
            &[
                "SELECT * FROM t WHERE a = 1 AND b = 'x'",
                "SELECT * FROM t WHERE a = 1 OR b = 'x'",
                "SELECT * FROM u WHERE a = 1 AND b = 'x'",
            ],
            // A digit inside a name is part of the name.
            &["SELECT 1 FROM t1", "SELECT 1 FROM t2"],
            // Only an IN list collapses, and only a run of placeholders.
            &[
                "INSERT INTO t VALUES (1)",
                "INSERT INTO t VALUES (1, 2)",
                "INSERT INTO t VALUES (1, 2, 3)",
            ],
            &[
                "SELECT * FROM t WHERE a IN (1, 2)",
                "SELECT * FROM t WHERE a NOT IN (1, 2)",
                "SELECT * FROM t WHERE a IN (1, b)",
                "SELECT * FROM t WHERE a IN (SELECT b FROM u)",
                "SELECT * FROM t WHERE a IN (1) AND b IN (2, 3, c)",
            ],
            // A hex number's `e` is a digit: the sign after it is an operator.
            &["SELECT 0xe-1", "SELECT 5", "SELECT 0xe - c"],
            &["SELECT 1e", "SELECT 1 e"],
        ];
        for group in groups {
            let values: Vec<u64> = group.iter().map(|sql| fingerprint(sql)).collect();
            for (index, value) in values.iter().enumerate() {
                assert!(
                    !values[..index].contains(value),
                    "{} repeats an earlier fingerprint",
                    group[index]
                );
            }
        }
    }
}
