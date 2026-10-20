use datafusion::sql::sqlparser::ast::{
    Expr, LimitClause, Query, SetExpr, TopQuantity, Value, VisitorMut,
};
use std::ops::ControlFlow;

/// An sqlparser AST visitor, which transforms TOP to LIMIT
pub(super) struct TopToLimit;

impl TopToLimit {
    fn set_limit(
        limit_clause: &mut Option<LimitClause>,
        quantity: u64,
    ) -> ControlFlow<&'static str> {
        let new_limit = Some(Expr::value(Value::Number(quantity.to_string(), false)));

        match limit_clause.as_mut() {
            // OFFSET is supported by ADQL, but not LIMIT. We skip the entire processing
            // we want this to be idempotent, and LIMIT must be caught by another visitor.
            Some(LimitClause::LimitOffset { limit: Some(_), .. }) => ControlFlow::Continue(()),
            // Set limit here
            Some(LimitClause::LimitOffset {
                limit: limit @ None,
                ..
            }) => {
                *limit = new_limit;
                ControlFlow::Continue(())
            }
            // Not supported by ADQL
            Some(LimitClause::OffsetCommaLimit { .. }) => ControlFlow::Break(
                "ADQL does not support OFFSET,LIMIT syntax (with comma), use 'SELECT TOP 10 ... FROM ... OFFSET 10' instead",
            ),
            None => {
                *limit_clause = Some(LimitClause::LimitOffset {
                    limit: new_limit,
                    offset: None,
                    limit_by: vec![],
                });
                ControlFlow::Continue(())
            }
        }
    }

    /// Wrap "SELECT TOP ..." with a subquery, so when TOP being converted to LIMIT it occurs in
    /// the right place.
    fn wrap_select_with_top(set_expr: &mut SetExpr) {
        match set_expr {
            SetExpr::SetOperation { left, right, .. } => {
                Self::wrap_select_with_top(left);
                Self::wrap_select_with_top(right);
            }
            // Replace the SELECT with a subquery containing it
            SetExpr::Select(select) if select.top.is_some() => {
                *set_expr = SetExpr::Query(Box::new(Query {
                    with: None,
                    body: Box::new(set_expr.clone()),
                    order_by: None,
                    limit_clause: None,
                    fetch: None,
                    locks: vec![],
                    for_clause: None,
                    settings: None,
                    format_clause: None,
                    pipe_operators: vec![],
                }));
            }
            // Parenthesized operands are queries, they are visited by the visitor
            _ => {}
        }
    }
}

impl VisitorMut for TopToLimit {
    type Break = &'static str;

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        // The visitor will visit the new subqueries later and convert their TOP
        if let SetExpr::SetOperation { left, right, .. } = query.body.as_mut() {
            Self::wrap_select_with_top(left);
            Self::wrap_select_with_top(right);
        }
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

            Self::set_limit(&mut query.limit_clause, quantity)
        } else {
            ControlFlow::Continue(())
        }
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
            (
                "SELECT TOP 3 a FROM x UNION SELECT a FROM y",
                "(SELECT a FROM x LIMIT 3) UNION SELECT a FROM y",
            ),
            (
                "SELECT a FROM x UNION SELECT TOP 3 a FROM y EXCEPT SELECT TOP 5 a FROM z",
                "SELECT a FROM x UNION (SELECT a FROM y LIMIT 3) EXCEPT (SELECT a FROM z LIMIT 5)",
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
