//! ADQL query parsing.
//!
//! [`AdqlDialect`] is an sqlparser dialect used with DataFusion's SQL parser. It is a minimal
//! implementation for now: besides identifiers, it relies on sqlparser defaults, so it still
//! accepts non-ADQL syntax (e.g. `LIMIT`), reserves non-ADQL keywords, and doesn't support
//! some ADQL syntax (e.g. `TOP` is parsed, but not planned by DataFusion).

use datafusion::common::{Result, not_impl_err, plan_err};
use datafusion::execution::SessionState;
use datafusion::prelude::*;
use datafusion::sql::parser::{DFParserBuilder, Statement};
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
fn parse_adql(state: &SessionState, query: &str) -> Result<Statement> {
    let recursion_limit = state.config_options().sql_parser.recursion_limit.get();
    let mut statements = DFParserBuilder::new(query)
        .with_dialect(&AdqlDialect)
        .with_recursion_limit(recursion_limit)
        .build()?
        .parse_statements()?;
    if statements.len() > 1 {
        return not_impl_err!("Only a single ADQL query is supported");
    }
    match statements.pop_front() {
        Some(statement) => Ok(statement),
        None => plan_err!("No ADQL query was provided"),
    }
}

/// ADQL counterpart of [`SessionContext::sql`]
///
/// DataFusion doesn't support registering custom parser dialects, so we parse with
/// [`AdqlDialect`] ourselves and pass the statement to DataFusion's planner.
pub(crate) trait AdqlContext {
    /// Creates a DataFrame for the ADQL query.
    ///
    /// Only read-only queries are allowed: DDL, DML and other statements are rejected.
    async fn adql(&self, query: &str) -> Result<DataFrame>;
}

impl AdqlContext for SessionContext {
    async fn adql(&self, query: &str) -> Result<DataFrame> {
        let state = self.state();
        let statement = parse_adql(&state, query)?;
        let plan = state.statement_to_plan(statement).await?;
        SQLOptions::new()
            .with_allow_ddl(false)
            .with_allow_dml(false)
            .with_allow_statements(false)
            .verify_plan(&plan)?;
        self.execute_logical_plan(plan).await
    }
}
