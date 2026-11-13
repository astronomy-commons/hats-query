//! ADQL identifiers.

use super::parser::AdqlDialect;
use datafusion::sql::sqlparser::dialect::Dialect;
use std::borrow::Cow;

/// ADQL 2.1 Section 2.1.3 "SQL reserved keywords", without END-EXEC, which can't be an
/// identifier anyway. Sorted for binary search.
const SQL_RESERVED_KEYWORDS: &[&str] = &[
    "ABSOLUTE",
    "ACTION",
    "ADD",
    "ALL",
    "ALLOCATE",
    "ALTER",
    "AND",
    "ANY",
    "ARE",
    "AS",
    "ASC",
    "ASSERTION",
    "AT",
    "AUTHORIZATION",
    "AVG",
    "BEGIN",
    "BETWEEN",
    "BIT",
    "BIT_LENGTH",
    "BOTH",
    "BY",
    "CASCADE",
    "CASCADED",
    "CASE",
    "CAST",
    "CATALOG",
    "CHAR",
    "CHARACTER",
    "CHARACTER_LENGTH",
    "CHAR_LENGTH",
    "CHECK",
    "CLOSE",
    "COALESCE",
    "COLLATE",
    "COLLATION",
    "COLUMN",
    "COMMIT",
    "CONNECT",
    "CONNECTION",
    "CONSTRAINT",
    "CONSTRAINTS",
    "CONTINUE",
    "CONVERT",
    "CORRESPONDING",
    "COUNT",
    "CREATE",
    "CROSS",
    "CURRENT",
    "CURRENT_DATE",
    "CURRENT_TIME",
    "CURRENT_TIMESTAMP",
    "CURRENT_USER",
    "CURSOR",
    "DATE",
    "DAY",
    "DEALLOCATE",
    "DECIMAL",
    "DECLARE",
    "DEFAULT",
    "DEFERRABLE",
    "DEFERRED",
    "DELETE",
    "DESC",
    "DESCRIBE",
    "DESCRIPTOR",
    "DIAGNOSTICS",
    "DISCONNECT",
    "DISTINCT",
    "DOMAIN",
    "DOUBLE",
    "DROP",
    "ELSE",
    "END",
    "ESCAPE",
    "EXCEPT",
    "EXCEPTION",
    "EXEC",
    "EXECUTE",
    "EXISTS",
    "EXTERNAL",
    "EXTRACT",
    "FALSE",
    "FETCH",
    "FIRST",
    "FLOAT",
    "FOR",
    "FOREIGN",
    "FOUND",
    "FROM",
    "FULL",
    "GET",
    "GLOBAL",
    "GO",
    "GOTO",
    "GRANT",
    "GROUP",
    "HAVING",
    "HOUR",
    "IDENTITY",
    "IMMEDIATE",
    "IN",
    "INDICATOR",
    "INITIALLY",
    "INNER",
    "INPUT",
    "INSENSITIVE",
    "INSERT",
    "INT",
    "INTEGER",
    "INTERSECT",
    "INTERVAL",
    "INTO",
    "IS",
    "ISOLATION",
    "JOIN",
    "KEY",
    "LANGUAGE",
    "LAST",
    "LEADING",
    "LEFT",
    "LEVEL",
    "LIKE",
    "LOCAL",
    "LOWER",
    "MATCH",
    "MAX",
    "MIN",
    "MINUTE",
    "MODULE",
    "MONTH",
    "NAMES",
    "NATIONAL",
    "NATURAL",
    "NCHAR",
    "NEXT",
    "NO",
    "NOT",
    "NULL",
    "NULLIF",
    "NUMERIC",
    "OCTET_LENGTH",
    "OF",
    "ON",
    "ONLY",
    "OPEN",
    "OPTION",
    "OR",
    "ORDER",
    "OUTER",
    "OUTPUT",
    "OVERLAPS",
    "PAD",
    "PARTIAL",
    "POSITION",
    "PRECISION",
    "PREPARE",
    "PRESERVE",
    "PRIMARY",
    "PRIOR",
    "PRIVILEGES",
    "PROCEDURE",
    "PUBLIC",
    "READ",
    "REAL",
    "REFERENCES",
    "RELATIVE",
    "RESTRICT",
    "REVOKE",
    "RIGHT",
    "ROLLBACK",
    "ROWS",
    "SCHEMA",
    "SCROLL",
    "SECOND",
    "SECTION",
    "SELECT",
    "SESSION",
    "SESSION_USER",
    "SET",
    "SIZE",
    "SMALLINT",
    "SOME",
    "SPACE",
    "SQL",
    "SQLCODE",
    "SQLERROR",
    "SQLSTATE",
    "SUBSTRING",
    "SUM",
    "SYSTEM_USER",
    "TABLE",
    "TEMPORARY",
    "THEN",
    "TIME",
    "TIMESTAMP",
    "TIMEZONE_HOUR",
    "TIMEZONE_MINUTE",
    "TO",
    "TRAILING",
    "TRANSACTION",
    "TRANSLATE",
    "TRANSLATION",
    "TRIM",
    "TRUE",
    "UNION",
    "UNIQUE",
    "UNKNOWN",
    "UPDATE",
    "UPPER",
    "USAGE",
    "USER",
    "USING",
    "VALUE",
    "VALUES",
    "VARCHAR",
    "VARYING",
    "VIEW",
    "WHEN",
    "WHENEVER",
    "WHERE",
    "WITH",
    "WORK",
    "WRITE",
    "YEAR",
    "ZONE",
];

/// ADQL 2.1 Section 2.1.4 "ADQL reserved keywords". Sorted for binary search.
const ADQL_RESERVED_KEYWORDS: &[&str] = &[
    "ABS",
    "ACOS",
    "AREA",
    "ASIN",
    "ATAN",
    "ATAN2",
    "BIGINT",
    "BOX",
    "CEILING",
    "CENTROID",
    "CIRCLE",
    "CONTAINS",
    "COORD1",
    "COORD2",
    "COORDSYS",
    "COS",
    "COT",
    "DEGREES",
    "DISTANCE",
    "EXP",
    "FLOOR",
    "ILIKE",
    "INTERSECTS",
    "IN_UNIT",
    "LOG",
    "LOG10",
    "MOD",
    "OFFSET",
    "PI",
    "POINT",
    "POLYGON",
    "POWER",
    "RADIANS",
    "RAND",
    "REGION",
    "ROUND",
    "SIN",
    "SQRT",
    "TAN",
    "TOP",
    "TRUNCATE",
];

/// Returns the name as it must be written in ADQL queries.
///
/// Names which are not regular identifiers or are reserved keywords are delimited with double
/// quotes, see ADQL 2.1 Sections 2.1.5 and 2.1.6. Regular identifiers are case-insensitive,
/// and DataFusion folds them to lower case, so names with upper case letters are delimited too.
pub(crate) fn delimit_if_needed(name: &str) -> Cow<'_, str> {
    if is_lowercase_regular_identifier(name) && !is_reserved_keyword(name) {
        Cow::Borrowed(name)
    } else {
        Cow::Owned(format!("\"{}\"", name.replace('"', "\"\"")))
    }
}

fn is_lowercase_regular_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|ch| AdqlDialect.is_identifier_start(ch))
        && chars.all(|ch| AdqlDialect.is_identifier_part(ch))
        && !name.chars().any(|ch| ch.is_ascii_uppercase())
}

/// Case-insensitive lookup in the reserved keyword lists.
fn is_reserved_keyword(name: &str) -> bool {
    [SQL_RESERVED_KEYWORDS, ADQL_RESERVED_KEYWORDS]
        .iter()
        .any(|keywords| {
            keywords
                .binary_search_by(|keyword| {
                    keyword
                        .bytes()
                        .cmp(name.bytes().map(|b| b.to_ascii_uppercase()))
                })
                .is_ok()
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keywords_are_sorted() {
        assert!(SQL_RESERVED_KEYWORDS.is_sorted());
        assert!(ADQL_RESERVED_KEYWORDS.is_sorted());
    }

    #[test]
    fn regular() {
        for name in ["ra", "ra_error", "mag2", "x", "coord"] {
            assert!(
                matches!(delimit_if_needed(name), Cow::Borrowed(n) if n == name),
                "{name}"
            );
        }
    }

    #[test]
    fn delimited() {
        for (name, expected) in [
            ("_healpix_29", r#""_healpix_29""#),
            ("2mass", r#""2mass""#),
            ("RA", r#""RA""#),
            ("Ra", r#""Ra""#),
            ("INTERSECTS12", r#""INTERSECTS12""#),
            ("a b", r#""a b""#),
            (r#"a"b"#, r#""a""b""#),
            ("", r#""""#),
        ] {
            assert_eq!(delimit_if_needed(name), expected, "{name}");
        }
    }

    #[test]
    fn reserved() {
        for (name, expected) in [
            ("size", r#""size""#),
            ("SIZE", r#""SIZE""#),
            ("absolute", r#""absolute""#),
            ("zone", r#""zone""#),
            ("select", r#""select""#),
            ("point", r#""point""#),
            ("intersects", r#""intersects""#),
            ("top", r#""top""#),
            ("in_unit", r#""in_unit""#),
        ] {
            assert_eq!(delimit_if_needed(name), expected, "{name}");
        }
    }

    #[test]
    fn not_reserved() {
        for name in ["coord", "intersects12", "value_", "sizes"] {
            assert!(!is_reserved_keyword(name), "{name}");
        }
    }
}
