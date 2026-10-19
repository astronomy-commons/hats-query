//! ADQL query parsing.
//!
//! [`AdqlDialect`] is an sqlparser dialect used with DataFusion's SQL parser. It is a minimal
//! implementation for now: besides identifiers, it relies on sqlparser defaults, so it still
//! accepts non-ADQL syntax (e.g. `LIMIT`), reserves non-ADQL keywords, and doesn't support
//! some ADQL syntax.

use crate::adql::top::TopToLimit;
use datafusion::common::{Result as DFResult, not_impl_err, plan_err};
use datafusion::execution::SessionState;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::*;
use datafusion::sql::parser::{DFParserBuilder, Statement};
use datafusion::sql::sqlparser::ast::VisitMut;
use datafusion::sql::sqlparser::dialect::Dialect;

/// sqlparser dialect for ADQL 2.1
#[derive(Debug, Default)]
pub(crate) struct AdqlDialect;

impl Dialect for AdqlDialect {
    /// ADQL 2.1 Section 2.1.3: delimited identifiers are enclosed in double quotes only
    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '"'
    }

    /// ADQL 2.1 Section 2.1.3: regular identifiers start with a Latin letter
    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_ascii_alphabetic()
    }

    /// ADQL 2.1 Section 2.1.3: regular identifiers consist of Latin letters, digits and underscores
    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_ascii_alphanumeric() || ch == '_'
    }
}

/// Parses a single ADQL query into a statement.
fn parse_adql(state: &SessionState, query: &str) -> DFResult<Statement> {
    let recursion_limit = state.config_options().sql_parser.recursion_limit.get();
    let mut statements = DFParserBuilder::new(query)
        .with_dialect(&AdqlDialect)
        .with_recursion_limit(recursion_limit)
        .build()?
        .parse_statements()?;
    if statements.len() > 1 {
        return not_impl_err!("Only a single ADQL query is supported");
    }
    let mut statement = match statements.pop_front() {
        Some(statement) => statement,
        None => return plan_err!("No ADQL query was provided"),
    };
    if let Statement::Statement(sql_statement) = &mut statement
        && sql_statement.visit(&mut TopToLimit).is_break()
    {
        return plan_err!("Invalid ADQL TOP clause");
    }
    Ok(statement)
}

async fn adql_query_to_plan(
    session_context: &SessionContext,
    query: &str,
) -> DFResult<LogicalPlan> {
    let state = session_context.state();
    let statement = parse_adql(&state, query)?;
    let plan = state.statement_to_plan(statement).await?;
    SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false)
        .verify_plan(&plan)?;
    Ok(plan)
}

/// ADQL counterpart of [`SessionContext::sql`]
///
/// DataFusion doesn't support registering custom parser dialects, so we parse with
/// [`AdqlDialect`] ourselves and pass the statement to DataFusion's planner.
pub(crate) trait AdqlContext {
    /// Creates a DataFrame for the ADQL query.
    ///
    /// Only read-only queries are allowed: DDL, DML and other statements are rejected.
    async fn adql(&self, query: &str) -> DFResult<DataFrame>;
}

impl AdqlContext for SessionContext {
    async fn adql(&self, query: &str) -> DFResult<DataFrame> {
        let plan = adql_query_to_plan(self, query).await?;
        self.execute_logical_plan(plan).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Runs ADQL tests defined in the Lyonetia XML files
    mod lyonetia_xmls {
        use super::*;
        use crate::adql::register_adql;
        use datafusion::common::{DataFusionError, SchemaError};
        use futures_util::{StreamExt, stream};
        use roxmltree::Document;
        use std::path::{Path, PathBuf};

        /// Lyonetia queries known to fail the grammar test, please update when the parser is fixed
        /// to pass them
        const KNOWN_TO_FAIL_GRAMMAR: &[&str] = &[
            "select x fromy where Point('fk5', 2, 3)=x",
            "select x from y where Point ('fk5',2,3)",
            "SELECT distance FROM stars",
            "SELECT * FROM t1 INNER JOIN t2",
            "select x from y where Point('fk5',2,3)",
            "select x from y where CIRCLE('fk5', 2, 3)=x",
            "select x from y where POLYGON('fk5', 2, 3, 3, 0, 23, 0, 45)=x",
            "select * FROM y WHERE CONTAINS(a,b)",
            "SELECT id, my_undefined_function(ra, dec) AS bad FROM atable",
            "SELECT *\n        FROM (\n          WITH alpha_subset AS (\n            SELECT *\n            FROM stars\n            WHERE mod(id, 10) = 0\n          )\n          SELECT ra, dec\n          FROM alpha_subset\n          WHERE ra > 10 AND ra < 20\n        ) AS db_stars\n          INNER JOIN TAP_UPLOAD.my_stars\n            ON 1=CONTAINS(POINT(db_stars.ra, db_stars.dec),\n                          CIRCLE(my_stars.ra, my_stars.dec, 1./60))",
            "SELECT id FROM table1 WHERE id <= 10\nUNION\n    (\n        WITH tt AS (SELECT id FROM table1 WHERE id > 120406)\n        SELECT id FROM tt\n    )",
            "SELECT name, CAST(123 AS NULL)\n        FROM stars",
            "SELECT name, CAST(314 AS FLOAT)\n        FROM stars",
            "SELECT COALESCE()\n        FROM stars",
            "SELECT name, ra, dec\n        FROM stars\n        OFFSET -10",
            "SELECT name, ra, dec\n        FROM stars\n        OFFSET 10.5",
        ];

        /// Lyonetia queries known to fail the planning test, please update when the planner is fixed
        /// to pass them
        const KNOWN_TO_FAIL_PLANNING: &[&str] = &[
            "select top 3 * from x \n\t\tunion (\n\t\tselect top 40 a from y \n\t\t\texcept select * from z\n\t\t)",
        ];

        #[derive(Debug)]
        struct TestCase {
            query: String,
            #[allow(dead_code)] // Used in test report via Debug
            description: String,
            #[allow(dead_code)] // Used in test report via Debug
            version: String,
            valid: bool,
        }

        #[derive(Debug)]
        struct TestFailure {
            #[allow(dead_code)] // Used in test report via Debug
            result: DFResult<()>,
            case: TestCase,
        }

        /// Sorted paths of the Lyonetia IVOA XML files
        fn get_xml_paths() -> Vec<PathBuf> {
            let ivoa_dir =
                Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/lyonetia/src/adql/ivoa");
            let mut paths: Vec<_> = std::fs::read_dir(&ivoa_dir)
                .expect("Failed to read Lyonetia IVOA directory")
                .map(|entry| {
                    let entry = entry.expect("Failed to read Lyonetia IVOA directory entry");
                    entry.path()
                })
                .filter(|path| path.extension().filter(|&ext| ext == "xml").is_some())
                .collect();
            paths.sort_unstable();
            paths
        }

        /// Test cases from a single Lyonetia XML file
        fn get_test_cases(path: &Path) -> Vec<TestCase> {
            let text = std::fs::read_to_string(path).expect("Failed to read Lyonetia XML file");
            let doc = Document::parse(&text).expect("Failed to parse Lyonetia XML file");
            doc.descendants()
                .filter(|node| node.has_tag_name("query"))
                .map(|node| {
                    let description = node
                        .descendants()
                        .find(|node| node.has_tag_name("description"))
                        .expect("Missing description element")
                        .text()
                        .expect("Missing description text")
                        .to_string();
                    let adql_node = node
                        .descendants()
                        .find(|node| node.has_tag_name("adql"))
                        .expect("Missing adql element");
                    let query = adql_node
                        .text()
                        .expect("Missing adql text")
                        .trim()
                        .to_string();
                    let version = adql_node
                        .attribute("version")
                        .expect("Missing version attribute")
                        .to_string();
                    let valid = match adql_node
                        .attribute("valid")
                        .expect("Missing valid attribute")
                    {
                        "true" => true,
                        "false" => false,
                        other => panic!("Invalid valid attribute value: {}", other),
                    };
                    TestCase {
                        query,
                        description,
                        version,
                        valid,
                    }
                })
                .collect()
        }

        /// Test cases from all Lyonetia IVOA XML files
        fn get_all_test_cases() -> Vec<TestCase> {
            get_xml_paths()
                .iter()
                .flat_map(|path| get_test_cases(path))
                .collect()
        }

        /// Compares the parsing or planning result with the expected validity of the query
        fn check_case<T>(case: TestCase, result: DFResult<T>) -> Result<(), TestFailure> {
            match (result, case.valid) {
                // pass, valid as expected
                (Ok(_), true) => Ok(()),
                // pass, invalid as expected
                (Err(_), false) => Ok(()),
                // fail, we mark as invalid, but ADQL query satisfies the standard
                (Err(err), true) => Err(TestFailure {
                    result: Err(err),
                    case,
                }),
                // fail, we mark as valid, but ADQL query doesn't satisfy the standard
                (Ok(_), false) => Err(TestFailure {
                    result: Ok(()),
                    case,
                }),
            }
        }

        /// Planning error caused by a table or column missing from the (empty) catalog
        fn is_unknown_name_error(err: &DataFusionError) -> bool {
            match err.find_root() {
                DataFusionError::SchemaError(err, _) => {
                    matches!(**err, SchemaError::FieldNotFound { .. })
                }
                DataFusionError::Plan(msg) => {
                    msg.starts_with("table '") && msg.ends_with("' not found")
                }
                _ => false,
            }
        }

        /// Prints the pass count and panics if any query not in `known_to_fail` failed
        fn test_report(results: Vec<Result<(), TestFailure>>, known_to_fail: &[&str]) {
            let n_total = results.len();

            let failed = results
                .into_iter()
                .filter_map(|res| res.err())
                .collect::<Vec<_>>();
            eprintln!(
                "Lyonetia test report: {} / {} tests passed",
                n_total - failed.len(),
                n_total
            );

            // Make a report of the tests which failed and not in the known to fail list and panic with it
            let unexpected_failures: Vec<_> = failed
                .into_iter()
                .filter(|failure| !known_to_fail.contains(&failure.case.query.as_str()))
                .collect();
            if !unexpected_failures.is_empty() {
                println!("Unexpected test failures:\n{:#?}", unexpected_failures);
                let queries = unexpected_failures
                    .iter()
                    .map(|failure| failure.case.query.as_str())
                    .collect::<Vec<_>>();
                panic!(
                    "{} of total {} tests failed unexpectedly, see the full report with `cargo test lyonetia -- --nocapture`: {:#?}",
                    queries.len(),
                    n_total,
                    queries
                );
            }
        }

        /// Plans Lyonetia queries; unknown tables and columns are not considered failures.
        /// This can give false positives when an unknown table or column is found before
        /// the actual error, but it allows testing the planner without actual data.
        /// Invalid queries are skipped, they are covered by `test_grammar`.
        #[tokio::test]
        async fn test_parser() {
            let ctx = SessionContext::new();
            register_adql(&ctx);

            let valid_cases = get_all_test_cases().into_iter().filter(|case| case.valid);
            let results = stream::iter(valid_cases)
                .then(async |case| {
                    let result = match adql_query_to_plan(&ctx, &case.query).await {
                        Err(err) if !is_unknown_name_error(&err) => Err(err),
                        _ => Ok(()),
                    };
                    check_case(case, result)
                })
                .collect::<Vec<_>>()
                .await;

            test_report(results, KNOWN_TO_FAIL_PLANNING);
        }

        /// Parses Lyonetia queries, checking ADQL grammar only
        #[test]
        fn test_grammar() {
            let state = SessionContext::new().state();

            let results = get_all_test_cases()
                .into_iter()
                .map(|case| {
                    let result = parse_adql(&state, &case.query);
                    check_case(case, result)
                })
                .collect();

            test_report(results, KNOWN_TO_FAIL_GRAMMAR);
        }
    }
}
