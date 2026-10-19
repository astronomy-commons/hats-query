//! ADQL query parsing.
//!
//! [`AdqlDialect`] is an sqlparser dialect used with DataFusion's SQL parser. It is a minimal
//! implementation for now: besides identifiers, it relies on sqlparser defaults, so it still
//! accepts non-ADQL syntax (e.g. `LIMIT`), reserves non-ADQL keywords, and doesn't support
//! some ADQL syntax (e.g. `TOP` is parsed, but not planned by DataFusion).

use datafusion::common::{Result as DFResult, not_impl_err, plan_err};
use datafusion::execution::SessionState;
use datafusion::logical_expr::LogicalPlan;
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
    match statements.pop_front() {
        Some(statement) => Ok(statement),
        None => plan_err!("No ADQL query was provided"),
    }
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
            "select\t\tdistinct top\n\n\n    10 x, y from foo",
            "select top 3 * from x \n\t\tunion (\n\t\tselect top 40 a from y \n\t\t\texcept select * from z\n\t\t)",
            "select * from (\n\t\tselect TOP 30 * from x\n\t\t) as q \n\t\tunion\n\t\tselect TOP 4 * from u",
            "select top 4* from (select * from amanda.nucand)x",
            "SELECT TOP 10 name, ra, dec\n      FROM stars",
            "SELECT TOP 0 name, ra, dec\n      FROM stars",
            "SELECT TOP 1\n        abs(-3.14)                               AS \"ABS\",\n        truncate(ceiling(313.6)/100, 2)          AS \"CEILING\",\n        truncate(degrees(0.054803338512622), 2)  AS \"DEGREES\",\n        truncate(exp(1.1442227999201620), 2)     AS \"EXP\",\n        truncate(floor(314.3)/100, 2)            AS \"FLOOR\",\n        truncate(log(23.103866858722183), 2)     AS \"LOG\",\n        truncate(log10(1380.3842646028848), 2)   AS \"LOG10\",\n        mod(7.14, 4)                             AS \"MOD\",\n        truncate(pi(), 2)                        AS \"PI\",\n        round(power(1.772004514666935, 2), 2)    AS \"POW\",\n        round(radians(179.908747671078)*100)/100 AS \"RADIANS\",\n        truncate(sqrt(9.8596000000000000), 2)    AS \"SQRT\",\n        round(3.14278210, 2)                     AS \"ROUND\",\n        truncate(3.1499999999, 2)                AS \"TRUNCATE\"\n      FROM stars",
            "SELECT TOP 1\n        acos(1)            AS \"ACOS\",\n        asin(0)            AS \"ASIN\",\n        atan(0)            AS \"ATAN\",\n        atan2(0,1)         AS \"ATAN2\",\n        round(cos(pi()/2)) AS \"COS\",\n        sin(0)             AS \"SIN\",\n        tan(0)             AS \"TAN\"\n      FROM stars",
            "select TOP 10 * FROM spatial\n\tWHERE 1=CONTAINS(POINT('ICRS', ra1, ra2),\n\t\tCIRCLE('ICRS', 10, 10, 0.5))",
            "select TOP 10 * FROM spatial\nWHERE 1=INTERSECTS(CIRCLE('ICRS', 10, 10, 0.5),\n\t\tPOINT('ICRS', ra1, ra2))",
            "-- take the 10 first\n  ( SELECT TOP 10 id, ra, dec FROM atable ORDER BY id ASC)\nUNION\n  -- take the 10 last\n  ( SELECT TOP 10 id, ra, dec FROM atable ORDER BY id DESC)",
            "WITH tenFirst AS ( SELECT TOP 10 id, ra, dec FROM atable ORDER BY id ASC),\n      tenLast AS ( SELECT TOP 10 id, ra, dec FROM atable ORDER BY id DESC)\n  SELECT * FROM tenFirst\nUNION\n  SELECT * FROM tenLast",
            "-- Use case name: 'obs wavelength_coverage 1'\n\nSELECT TOP 200 target_name, instrument_name, dataproduct_type,\n   em_min, em_max, em_res_power, obs_collection, dataproduct_subtype \nFROM ivoa.ObsCore \nWHERE em_min <= 2.18E-6 \n  AND em_max >= 2.18E-6 \n  AND s_dec < -74.5\n-- and em_res_power < 10 -- to exclude narrow-band filters like the Hawki-BrGamma\nORDER BY s_dec\n-- \n-- Find products containing the 2.18 micron wavelength,\n-- which are closer than 15.5 degrees to the south pole (FK5 J2000).\n-- Limit the results to the first 200 records.\n-- Expected results:\n--   . Any spectrum, cube, or visibility, whose wavelengths extend below and above the 2.18 micron\n--   . Any image, source table, catalog, whose filter covers the 2.18 micron wavelength\n--     (i.e. the Ks-filter of all instruments, the BrGamma narrow-band filter of HAWKY), \n--     (see Helper: Characteristics of ESO filters below)\n--   . Only products whose declination is southern than -74.5 degrees (FK5 J2000) are considered.\n--   . ORDER BY: the results will be sorted by the declination in the default ascending order.\n--   . TOP 200: only the first 200 records (at maximum) of the result set will be returned,\n--     even if more results are available.\n--   . The TOP operator is applied to the query result set, that is, after the sorting has taken place;\n--     effectively, this means that only the first 200 closest products to the south pole are returned.",
            "-- Use case name: 'obs wavelength_coverage 2'\n\n\nSELECT TOP 200 target_name, instrument_name, em_min, em_max, em_res_power, obs_id, obs_publisher_did \nFROM ivoa.Obscore \nWHERE dataproduct_type = 'spectrum'                 -- find 1d spectra with \n  AND em_min < 656.28E-9 AND em_max > 656.28E-9     -- wavelength covering the Halpha wavelength (in meters, at redshift 0.0)\n\n-- Find 200 1d spectra covering the Halpha wavelength (in meters, at redshift 0.0)\n\n-- The list of standard dataproduct_type values supported by the ESO archive are:\n-- 'spectrum', 'image', 'cube', 'measurements', 'visibility'\n-- Some ESO-specific types are provided via the dataproduct_subtype column.",
            "-- Use case name: 'obs spatial_query_footprint 3'\n\nSELECT TOP 100 * FROM ivoa.Obscore \nWHERE CONTAINS(POINT('ICRS',123.2675,-34.5785),s_region) = 1  -- footprint(the s_region field) must contain point (RA,DEC)=(123.2675deg,-34.5785deg)\n\n-- Find products containing the position of the cepheid RS PUPPIS\n-- Please note that this query very likely will NOT return any spectrum. \n-- That is because, in the ESO case, 1D spectra have point-like footprints. \n-- Refer and compare with ospfoot4.",
            "SELECT TOP 10 obs_publisher_did, instrument_name, em_min, em_max, em_res_power, obs_collection \nFROM ivoa.ObsCore \nWHERE em_min < 2.159E-6 AND em_max > 2.159E-6 -- contains the central lambda of Ks of the 2MASS-Cohen standard photometric system\nAND em_res_power between 5 and 10 -- and limits the resolving power to a small value (no spectrum/cube will match)\n                                  -- Expected are em_res_power(Ks)=6.22 of HAWKI, 7.1 of VIRCAM, 8.0 of ISAAC,\n                                  -- but not the BrGamma of HAWKI (69.85).\n                                  -- See the Helper tables below.",
            "SELECT TOP 100 instrument_name, em_min, em_max, s_resolution, distance(s_region, point('',266.42,-29.0)) as dist_from_GC, dataproduct_type \n FROM ivoa.Obscore \nWHERE dataproduct_type='image' \n AND INTERSECTS(CIRCLE('ICRS',266.42,-29.0,5),s_region) = 1  -- intersecting (aka overlapping) a 5 deg cone around the Galactic Centre\n AND ((em_min < 1.25E-6 AND em_max > 1.25E-6)   -- Contains lambda(J) or\n     OR (em_min < 1.65E-6 AND em_max > 1.65E-6) -- Contains lambda(H) or\n     OR (em_min < 2.2E-6 AND em_max > 2.2E-6))  -- Contains lambda(Ks) \n AND em_res_power < 20                          -- Low spectral resolving power, includes both images and source tables \n ORDER BY 4               -- 4 is the position of the s_resolution within fields in the SELECT",
            "SELECT TOP 50 instrument_name, em_min, em_max, s_resolution, distance(s_region, point('',266.42,-29.0)) as dist_footprint_from_GC \n FROM ivoa.Obscore \nWHERE dataproduct_type='image' \n AND INTERSECTS(CIRCLE('ICRS',266.42,-29.0,5),s_region) = 1 \n AND ((em_min < 1.265E-6 AND em_max > 1.265E-6) -- Contains lambda(J) \n     OR (em_min < 1.66E-6 AND em_max > 1.66E-6) -- Contains lambda(H) \n     OR (em_min < 2.18E-6 AND em_max > 2.18E-6)) -- Contains lambda(Ks) \n AND dataproduct_type = 'image' \n ORDER BY 5              -- 5 is the position in the SELECT of the distance from \n                         -- the galactic center of the returned footprint (aliased to dist_footprint_from_GC), in degrees)",
            "SELECT TOP 50 J.target_name, J.obs_publisher_did, J.s_ra, J.s_dec, J.em_min, J.em_max, J.instrument_name,\n              H.target_name, H.obs_publisher_did, H.s_ra, H.s_dec, H.em_min, H.em_max, H.instrument_name,\n              ESO_INTERSECTION(J.s_region,H.s_region) as OverlappingArea, J.s_region, H.s_region\nFROM\n     (SELECT * FROM ivoa.Obscore WHERE dataproduct_type='image'\n        AND INTERSECTS(CIRCLE('ICRS',266.42,-29.0,5),s_region) = 1\n        AND em_min < 1.265E-6 AND em_max > 1.265E-6\n     ) as J\n,\n     (SELECT * FROM ivoa.Obscore WHERE dataproduct_type='image'\n        AND INTERSECTS(CIRCLE('ICRS',266.42,-29.0,5),s_region) = 1\n        AND em_min < 1.66E-6 AND em_max > 1.66E-6\n     ) as H\nWHERE INTERSECTS(J.s_region, H.s_region)=1\n  AND ESO_INTERSECTION(J.s_region,H.s_region) > 1.79\nORDER by 1, OverlappingArea desc",
            "SELECT  TOP 1000 *\nFROM ivoa.Obscore\nWHERE dataproduct_type='image'\n AND INTERSECTS(CIRCLE('ICRS',266.42,-29.0,5), s_region) = 1\n AND em_min < 1.265E-6 AND em_max > 1.265E-6 -- Contains lambda(J)",
            "SELECT top 120\n   distance(s_region, point('',266.42,-29.0)) as dist_footprint, -- CORRECT\n   distance(point('',s_ra,s_dec),point('',266.42,-29.0)) as dist_ra_dec, -- INCORRECT\ndataproduct_type, s_ra, s_dec\nFROM ivoa.Obscore \nWHERE dataproduct_type='image' \nAND INTERSECTS(CIRCLE('ICRS',266.42,-29.0,5),s_region) = 1  -- intersecting (aka overlapping) a 5 deg cone around the Galactic Centre\nAND em_min < 1.25E-6 AND em_max > 1.25E-6   -- Contains lambda(J)\nORDER BY dist_footprint, dist_ra_dec\n-- \n-- CAUTION: Never use s_ra, s_dec when computing distances, use s_region instead!",
            "SELECT top 1 \nPOLYGON('', 0,80, 270,80, 180,80, 90,80) as myPolygon,\narea(POLYGON('', 0,80, 270,80, 180,80, 90,80)) as myArea,\ncontains( POINT('', 0, 90), POLYGON('', 0,80, 270,80, 180,80, 90,80) ) as isNorthPoleInside,\ncontains( POINT('', 0, -90), POLYGON('', 0,80, 270,80, 180,80, 90,80) ) as isSouthPoleInside\nFROM ivoa.ObsCore",
            "SELECT top 1 \nPOLYGON('', 0,-80, 90,-80, 180,-80, 270,-80) as myPolygon,\narea(POLYGON('', 0,-80, 90,-80, 180,-80, 270,-80)) as myArea,\ncontains( POINT('', 0, 90), POLYGON('', 0,-80, 90,-80, 180,-80, 270,-80) ) as isNorthPoleInside,\ncontains( POINT('', 0, -90), POLYGON('', 0,-80, 90,-80, 180,-80, 270,-80) ) as isSouthPoleInside\nFROM ivoa.ObsCore",
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
