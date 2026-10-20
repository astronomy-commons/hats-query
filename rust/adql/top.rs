use datafusion::sql::sqlparser::ast::{
    Expr, LimitClause, Query, SetExpr, TopQuantity, Value, VisitorMut,
};
use std::ops::ControlFlow;

/// An sqlparser AST visitor, which transforms TOP to LIMIT
pub(super) struct TopToLimit;

impl VisitorMut for TopToLimit {
    type Break = &'static str;

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        let top = match query.body.as_mut() {
            SetExpr::Select(select) => select.top.take(),
            _ => None,
        };
        // Set limit if given
        if let Some(top) = top {
            // ADQL doesn't support WITH TIES or PERCENT
            if top.with_ties || top.percent {
                return ControlFlow::Break(
                    "ADQL does not support WITH TIES or PERCENT in TOP clause",
                );
            }
            let quantity = match top.quantity {
                Some(TopQuantity::Constant(quantity)) => quantity,
                // ADQL doesn't support expressions in TOP
                Some(TopQuantity::Expr(_)) => {
                    return ControlFlow::Break(
                        "ADQL does not support expressions in TOP clause, specify a constant like TOP 10",
                    );
                }
                // TOP must specify a quantity
                None => {
                    return ControlFlow::Break("TOP clause must specify a quantity, like TOP 10");
                }
            };
            let new_limit = Some(Expr::value(Value::Number(quantity.to_string(), false)));

            match query.limit_clause.as_mut() {
                // OFFSET is supported by ADQL, but not LIMIT. We skip the entire processing
                // we want this to be idempotent, and LIMIT must be caught by another visitor.
                Some(LimitClause::LimitOffset { limit: Some(_), .. }) => {
                    return ControlFlow::Continue(());
                }
                // Set limit here
                Some(LimitClause::LimitOffset {
                    limit: limit @ None,
                    limit_by,
                    ..
                }) => {
                    // LIMIT BY is something supported by ClickHouse only, again should be caught
                    // elsewhere.
                    if !limit_by.is_empty() {
                        return ControlFlow::Continue(());
                    }
                    *limit = new_limit;
                }
                // Not supported by ADQL
                Some(LimitClause::OffsetCommaLimit { .. }) => {
                    return ControlFlow::Break(
                        "ADQL does not support OFFSET,LIMIT syntax (with comma), use 'SELECT TOP 10 ... FROM ... OFFSET 10' instead",
                    );
                }
                None => {
                    query.limit_clause = Some(LimitClause::LimitOffset {
                        limit: new_limit,
                        offset: None,
                        limit_by: vec![],
                    });
                }
            };
        }
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adql::parser::AdqlDialect;
    use datafusion::sql::sqlparser::ast::VisitMut;
    use datafusion::sql::sqlparser::parser::Parser;

    /// Parses an ADQL query, applies [`TopToLimit`] and prints the query back
    fn rewrite(query: &str) -> ControlFlow<&'static str, String> {
        let mut statements = Parser::parse_sql(&AdqlDialect, query).expect("Failed to parse query");
        statements.visit(&mut TopToLimit)?;
        // We assume single statement, because ADQL is a single-statement language
        ControlFlow::Continue(statements[0].to_string())
    }

    #[test]
    fn test_rewrites() {
        let inputs_outputs = [
            ("SELECT TOP 10 * FROM t", "SELECT * FROM t LIMIT 10"),
            (
                "SELECT TOP 0 * FROM t1 JOIN t2 ON t1.id = t2.id",
                "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id LIMIT 0",
            ),
            (
                "SELECT subquery.x * subquery.y AS xy FROM (SELECT TOP 5 * FROM t) AS subquery",
                "SELECT subquery.x * subquery.y AS xy FROM (SELECT * FROM t LIMIT 5) AS subquery",
            ),
            (
                "SELECT TOP 10 * FROM t ORDER BY x OFFSET 100",
                "SELECT * FROM t ORDER BY x LIMIT 10 OFFSET 100",
            ),
        ];
        for (input, expected) in inputs_outputs {
            let result = rewrite(input);
            match result {
                ControlFlow::Continue(output) => {
                    assert_eq!(output, expected, "Input query: '{input}'")
                }
                ControlFlow::Break(_) => panic!("Rewrite failed for {input}"),
            }
        }
    }
}
