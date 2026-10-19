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
        use futures_util::{StreamExt, stream};
        use roxmltree::Document;
        use std::path::{Path, PathBuf};

        /// Lyonetia XML test cases that are known to fail, please update when the parser is fixed
        /// to pass them
        const KNOWN_TO_FAIL: &[&str] = &[
            "select ROUND(5)as x from y",
            "select x from y where box( 'fk5', 2, 3, 3, 0 ) = x",
            "select x from y where CENTROID( CIRCLE('fk4', 2, 3, 3) )=x",
            "select x from y where CIRCLE( 'fk5', 2, 3, 7 ) =x",
            "select x from y where POLYGON( 'fk5' , 2 , 3 , 3 , 0 , 23 , 0 , 45, 11 ) = x",
            "select x from y where ROUND( 3 )=x",
            "SELECT POINT( 'ICRS' , 3, 4) FROM z",
            "select round(x,2)as a, truncate(x,-2)as b from foo",
            "SELECT ra1, dec, mass FROM\n\t( SELECT * FROM spatial ) as q LEFT OUTER JOIN spatial2\n\t\tUSING ( ra1, dist ) JOIN misc ON ( dist=mass )",
            "SELECT ra1, dec, mass FROM\n\t( spatial join spatial2 using (ra1) ), misc",
            "select\t\tdistinct top\n\n\n    10 x, y from foo",
            "select log10(x), log(x), rand(), rand(5), \n\t\t\tTRUNCATE(x), TRUNCATE(x,3) from foo",
            "select TAP_UPLOAD.\n\t\tabc\n\t\t.c from TAP_UPLOAD\n\t\t.\n\t\tabc",
            "select * from spatial join \n\t\t(\n\t\tselect ra1+dec, \n\t\tdist-2 as foo, dec from spatial2 \n\t\toffset 0) \n\t\tas q\n\t\tON ( \n\t\twidth = dec )",
            "select top 3 * from x \n\t\tunion (\n\t\tselect top 40 a from y \n\t\t\texcept select * from z\n\t\t)",
            "select * from (\n\t\tselect TOP 30 * from x\n\t\t) as q \n\t\tunion\n\t\tselect TOP 4 * from u",
            "select x from t1, t2, t3 WHERE \n\t\tt1\n\t\t.\n\t\tx\n\t\t=\n\t\tt2\n\t\t.\n\t\ty",
            "select x from t1 FULL OUTER JOIN t2 USING (\n\t\tx,\n\t\ty\n\t)",
            "SELECT LOWER( \n\t\t'foo'\n\t\t), \n\t\tLOWER(\n\t\tversion\n\t\t) from crazy",
            "select flag \n\t\t|| \n\t\t'ab'as cat from crazy",
            "SELECT * FROM misc JOIN (SELECT TOP 3 * FROM crazy) AS q ON (\n\t\tmag \n\t\t=\n\t\tq\n\t\t.\n\t\tct\n\t\t)",
            "SELECT ct FROM crazy JOIN ( \n  SELECT height FROM spatial\n  JOIN spatial2 ON (\n  ra2=dist\n  )\n    GROUP BY height\n    HAVING (height>avg(dist))) AS q \n\tON ( \n\twot=height\n\t)",
            "select alpha, rv from TAP_UPLOAD\n\t\t.\n\t\tadql",
            "select a,\n\t\tb \n\t\tfrom \n\t\t( select * from x ) \n\t\tq",
            "select all(a+b)X from \n  \t\t\tmost,(anything join else3 ON(a BETWEEN(b+and7)AND 4))",
            "select top 4* from (select * from amanda.nucand)x",
            "select * from bar group   by foo",
            "SELECT * FROM stars",
            "SELECT name, ra, dec\n      FROM stars",
            "SELECT \"_weird_name\", \"distance\"\n      FROM stars",
            "SELECT 'blabla bla' FROM stars",
            "SELECT 'Star/' || type\n      FROM stars",
            "SELECT 42,\n             -2,\n             3.14,\n             10e-5,\n             10E3\n      FROM stars",
            "SELECT radius*3.14, 1+2-6/2\n      FROM stars",
            "SELECT long AS ra, des AS \"Description\"\n      FROM stars",
            "SELECT DISTINCT type FROM stars",
            "SELECT ALL type FROM stars",
            "SELECT TOP 10 name, ra, dec\n      FROM stars",
            "SELECT TOP 0 name, ra, dec\n      FROM stars",
            "SELECT * FROM t1",
            "SELECT mytable.* FROM t1 AS mytable",
            "SELECT * FROM t1 NATURAL JOIN t2",
            "SELECT * FROM t1 INNER JOIN t2 USING(oid)",
            "SELECT * FROM t1 INNER JOIN t2 ON t1.oid = t2.name",
            "SELECT * FROM t1 JOIN t2 USING(oid)",
            "SELECT * FROM t1, t2",
            "SELECT * FROM t1 LEFT OUTER JOIN t2 USING(oid)",
            "SELECT * FROM t1 LEFT JOIN t2 USING(oid)",
            "SELECT * FROM t1 RIGHT OUTER JOIN t2 USING(oid)",
            "SELECT * FROM t1 RIGHT JOIN t2 USING(oid)",
            "SELECT * FROM t1 FULL OUTER JOIN t2 USING(oid)",
            "SELECT * FROM t1 FULL JOIN t2 USING(oid)",
            "SELECT *\n        FROM (SELECT * FROM t1 ) AS p",
            "SELECT *\n        FROM t1 JOIN (SELECT id, something FROM t1 WHERE something < 42) AS p USING(id)",
            "SELECT name\n        FROM stars\n        WHERE mag < 0",
            "SELECT name\n        FROM stars\n        WHERE ra BETWEEN -10 AND 10",
            "SELECT name\n        FROM stars\n        WHERE name = 'M31'",
            "SELECT name\n        FROM stars\n        WHERE name != 'M31'",
            "SELECT name\n        FROM stars\n        WHERE name <> 'M31'",
            "SELECT name\n        FROM stars\n        WHERE name < 'M31'",
            "SELECT name\n        FROM stars\n        WHERE name <= 'M31'",
            "SELECT name\n        FROM stars\n        WHERE name > 'M31'",
            "SELECT name\n        FROM stars\n        WHERE name >= 'M31'",
            "SELECT name\n        FROM stars\n        WHERE name LIKE 'NGC%'",
            "SELECT TOP 1\n        abs(-3.14)                               AS \"ABS\",\n        truncate(ceiling(313.6)/100, 2)          AS \"CEILING\",\n        truncate(degrees(0.054803338512622), 2)  AS \"DEGREES\",\n        truncate(exp(1.1442227999201620), 2)     AS \"EXP\",\n        truncate(floor(314.3)/100, 2)            AS \"FLOOR\",\n        truncate(log(23.103866858722183), 2)     AS \"LOG\",\n        truncate(log10(1380.3842646028848), 2)   AS \"LOG10\",\n        mod(7.14, 4)                             AS \"MOD\",\n        truncate(pi(), 2)                        AS \"PI\",\n        round(power(1.772004514666935, 2), 2)    AS \"POW\",\n        round(radians(179.908747671078)*100)/100 AS \"RADIANS\",\n        truncate(sqrt(9.8596000000000000), 2)    AS \"SQRT\",\n        round(3.14278210, 2)                     AS \"ROUND\",\n        truncate(3.1499999999, 2)                AS \"TRUNCATE\"\n      FROM stars",
            "SELECT TOP 1\n        acos(1)            AS \"ACOS\",\n        asin(0)            AS \"ASIN\",\n        atan(0)            AS \"ATAN\",\n        atan2(0,1)         AS \"ATAN2\",\n        round(cos(pi()/2)) AS \"COS\",\n        sin(0)             AS \"SIN\",\n        tan(0)             AS \"TAN\"\n      FROM stars",
            "SELECT COUNT(*) FROM stars",
            "SELECT COUNT(DISTINCT name)\n      FROM stars",
            "SELECT COUNT(*),\n             MIN(ra_error),\n             MAX(ra_error),\n             AVG(ra_error),\n             SUM(ra_error)\n      FROM stars",
            "SELECT type,\n             COUNT(*),\n             MIN(ra_error),\n             MAX(ra_error),\n             AVG(ra_error),\n             SUM(ra_error)\n      FROM stars\n      GROUP BY type\n      HAVING AVG(ra_error) > 0.001\n      ORDER BY 5 DESC",
            "SELECT id, ra, dec FROM atable ORDER BY id, ra ASC, dec DESC",
            "SELECT id, ra, dec FROM atable ORDER BY 1, 2 ASC, 3 DESC",
            "select x from t1 order by log10(z+1) desc, 2*x asc",
            "select x from y where Point(NULL, 2, 3)=x",
            "select x from y where Point('fk5', 2, 3)=x",
            "select x from y where CIRCLE('fk5', 2, 3, 3)=x",
            "select x from y where box('fk5', 2, 3, 3, 0)=x",
            "select x from y where POLYGON('fk5', 2, 3, 3, 0, 23, 0, 45, 34)=x",
            "select x from y where CENTROID(CIRCLE('fk4', 2, 3, 3))=x",
            "select x from y where Point(2,3)=x",
            "select * FROM t WHERE 1=CONTAINS(CIRCLE('ICRS', 4, 4, 2), POINT('', ra, dec))",
            "select point('ICRS', 1, 2) from spatial",
            "select POINT(3,4) FROM z",
            "select distance(point('galactic', 2, 3), point('ICRS', ra1, ra2)) from spatial",
            "select centroid(circle('galactic', ra1, ra2, 0.5)) from spatial",
            "select u.ra1 FROM spatial AS mine LEFT OUTER JOIN spatial2 as u ON (1=CONTAINS(POINT('', mine.ra1, mine.ra2), CIRCLE('', u.ra1, u.dec, 1)))",
            "select * from spatial as q join misc as p on (1=contains(point('ICRS', q.dist, q.width), circle('ICRS', p.mass, p.mag, 0.02)))",
            "select point('', ra2, 2) from spatial",
            "select point('', mag, 2) from misc",
            "select point(NULL, ra1, 2) from spatial",
            "select point('ICRS', ra2, 2) from spatial",
            "select coordsys(p), coord1(p), coord2(p) from\t(select point('FK5', ra1, width) as p from spatial) as q",
            "select region('Circle FK4 10 10 1') from spatial",
            "select POINT('ICRS', 5*width+height*LOG(width),height) from spatial",
            "select alphaFloat, deltaFloat from ppmx.data\nwhere contains(point('ICRS', alphaFloat, deltaFloat), \n\t\t\t\tcircle('ICRS', 23, 24, 0.2))=1",
            "select alphaFloat, deltaFloat from ppmx.data\nwhere 0=contains(point('ICRS', alphaFloat, deltaFloat),\n\t\t\t\tcircle('ICRS', 23, 24, 0.2))",
            "select TOP 10 * FROM spatial\n\tWHERE 1=CONTAINS(POINT('ICRS', ra1, ra2),\n\t\tCIRCLE('ICRS', 10, 10, 0.5))",
            "select TOP 10 * FROM spatial\nWHERE 1=INTERSECTS(CIRCLE('ICRS', 10, 10, 0.5),\n\t\tPOINT('ICRS', ra1, ra2))",
            "select TAP_UPLOAD.user_table.ra FROM\n\t\tTAP_UPLOAD.user_table WHERE (1=CONTAINS(POINT('ICRS',\n\t\tusnob.data.raj2000, usnob.data.dej2000), CIRCLE('ICRS',\n\t\tTAP_UPLOAD.user_table.ra2000, a.dec2000, 0.016666666666666666)))",
            "select * from foo where 1=CONTAINS(REGION('Position ICRS 1 2'), x)",
            "select * from foo where 1=CONTAINS(\n        REGION('Union ICRS (Position 1 2 Intersection (circle  1 2 3 box 1 2 3 4 circle 30 40 2))'),\n        REGION('circle GALACTIC 1 2 3')\n      )",
            "select * from foo where 1=INTERSECTS(REGION('NOT (circle  1 2 3)'), x)",
            "SELECT id, ivo_healpix_index(6, ra, dec) AS hpx FROM atable",
            "SELECT name\n        FROM stars\n        WHERE name ILIKE 'NgC %'",
            "SELECT LOWER(name)\n        FROM stars",
            "SELECT UPPER(name)\n        FROM stars",
            "WITH alpha_subset AS (\n          SELECT *\n          FROM stars\n          WHERE mod(id, 10) = 0\n        )\n        SELECT ra, dec\n        FROM alpha_subset\n        WHERE ra > 10 AND ra < 20",
            "SELECT id FROM table1 WHERE id <= 10\nUNION\n  SELECT id FROM table1 WHERE id > 120406",
            "SELECT id FROM table1 WHERE id <= 10\nUNION\n    SELECT TOP 10 id FROM table1 WHERE id > 10",
            "SELECT id, ra, dec FROM table1\nUNION\n  SELECT id, ra, dec FROM table2\nORDER BY id -- sort the UNION result",
            "-- take the 10 first\n  ( SELECT TOP 10 id, ra, dec FROM atable ORDER BY id ASC)\nUNION\n  -- take the 10 last\n  ( SELECT TOP 10 id, ra, dec FROM atable ORDER BY id DESC)",
            "SELECT id FROM table1 WHERE id <= 10\nINTERSECT\n    SELECT id FROM table1 WHERE id >= 10 AND mod(id, 2) = 0",
            "SELECT id FROM table1 WHERE id <= 10\nEXCEPT\n    SELECT id FROM table1 WHERE id >= 10 AND mod(id, 2) = 0",
            "SELECT id, ra, dec FROM table1\nUNION\n  SELECT id, ra, dec FROM table2\nINTERSECT\n  SELECT id, ra, dec FROM table3",
            "(\n    SELECT id FROM table1 WHERE id <= 10\nUNION\n    SELECT id FROM table1 WHERE id > 120406\n)\nINTERSECT\n    SELECT id FROM table1 WHERE (id <= 10 OR id > 120406) AND mod(id, 2) = 0",
            "WITH tenFirst AS ( SELECT TOP 10 id, ra, dec FROM atable ORDER BY id ASC),\n      tenLast AS ( SELECT TOP 10 id, ra, dec FROM atable ORDER BY id DESC)\n  SELECT * FROM tenFirst\nUNION\n  SELECT * FROM tenLast",
            "SELECT CAST(2022   AS SMALLINT),\n               CAST(5      AS INTEGER),\n               CAST(123456 AS BigInt),\n               CAST(3.14   AS REAL),\n               CAST(3.14   AS Double Precision)\n        FROM stars",
            "SELECT CAST(name AS CHAR),\n               CAST(name AS VarChar),\n               CAST(name AS char(30)),\n               CAST(name AS VARCHAR(30))\n        FROM stars",
            "SELECT name, CAST('2021-01-14T11:25:00' AS TIMESTAMP)\n        FROM stars",
            "SELECT CAST('12.3 45.6' AS POINT),\n               CAST('12.3 45.6 1.0' AS CIRCLE),\n               CAST('1.0 0.1 2.0 0.2 3.0 0.3' AS POLYGON)\n        FROM stars",
            "SELECT name, CAST(NULL AS VARCHAR)\n        FROM stars",
            "SELECT NULL\n        FROM stars",
            "SELECT COALESCE(star_name)\n        FROM stars",
            "SELECT COALESCE(favorite_name, star_name, 'a_star')\n        FROM stars",
            "SELECT name, IN_UNIT(ra, 'rad')\n        FROM stars",
            "SELECT name, IN_UNIT(sqrt(power(pm_ra,2)+power(pm_dec,2)), 'rad/yr')\n        FROM stars",
            "SELECT name, ra, dec\n        FROM stars\n        OFFSET 100",
            "-- Use case name: 'obs tap_schema 1'\n\nSELECT table_name, description\nFROM TAP_SCHEMA.tables\nORDER BY table_name\n\n-- Full explanation:\n-- All TAP services must support a set of (meta) tables, in a schema named TAP_SCHEMA.\n-- These meta-tables describe all tables and columns published through the TAP service.\n-- The meta-tables can be queried themselves via TAP.\n-- Users can discover ESO-published tables or columns by querying the tables in this schema.",
            "-- Use case name: 'obs tap_schema 2'\n\n\nSELECT column_name, datatype, arraysize, xtype, unit, description\nfrom TAP_SCHEMA.columns\nwhere table_name='dbo.ssa'\n\n-- The TAP_SCHEMA.columns table contains all column names, units,\n-- UCDs, description, etc., of all the published ESO tables.\n-- This use case answer to the following question:\n-- WHAT ARE THE COLUMNS OF THE SPECTRAL TABLE called dbo.ssa?\n-- \n-- Further details:\n-- Please note that string values must be embraced in single quotes (e.g.: 'dbo.ssa').",
            "-- Use case name: 'obs tap_schema 3'\n\nSELECT column_name, datatype, arraysize, xtype, unit, ucd, utype -- add description to know more\nfrom TAP_SCHEMA.columns\nwhere table_name='ivoa.ObsCore'\n\n-- WHAT ARE THE COLUMNS OF THE DATA PRODUCT TABLE called ivoa.ObsCore?\n-- The TAP_SCHEMA.columns table contains all column names, units,\n-- UCDs, description, etc., of all the published ESO tables.",
            "-- Use case name: 'obs tap_schema 4'\n\nSELECT column_name, datatype, arraysize, xtype, unit, ucd, description\nfrom TAP_SCHEMA.columns\nwhere table_name='dbo.raw'\n\n-- WHAT ARE THE COLUMNS OF THE TABLE describing the raw data?\n-- The TAP_SCHEMA.columns table contains all column names, units,\n-- UCDs, description, etc., of all the published ESO tables.",
            "-- Use case name: 'obs tap_schema 5 asm'\n\nSELECT table_name, description\nfrom TAP_SCHEMA.tables\nwhere schema_name='asm'\n\n-- WHAT ARE THE TABLES in the asm (Astronomical Site Monitoring) schema?\n-- In the asm schema you'll find all the meteorological, seeing, turbulence and\n-- and other atmospheric measurements taken in Paranal, La Silla, and Llano de Chanjantor.",
            "-- Use case name: 'obs spatial_query 1'\n\nSELECT count(*), dataproduct_type from ivoa.obscore\nwhere intersects(s_region, circle('', 191.288, -0.459, 0.2))=1\ngroup by dataproduct_type\norder by 1\n\n-- Search products whose footprint (s_region)\n-- intersects a circle, around the coordinates of ASASSN-14lp,\n-- of radius 0.2 degrees.\n-- To boost performances, always use s_region instead of s_ra, s_dec!",
            "-- Use case name: 'obs spatial_query 2'\n\nSELECT count(*), dataproduct_type from ivoa.obscore\nwhere contains(point('',s_ra, s_dec), circle('', 191.288, -0.459, 0.2))=1\ngroup by dataproduct_type\norder by 1\n\n-- Please refer and compare to  use case 'osp1'.\n-- To boost performances, always use s_region (when available) instead of s_ra, s_dec!",
            "-- Use case name: 'obs spatial_query 1'\n\nSELECT dp_id, instrument_name, em_min, em_max, t_min, t_max, abmaglim\nfrom ivoa.obscore\nwhere dataproduct_type='cube'\nand intersects(s_region, POLYGON('J2000', 13.0693,-71.65096, 14.84154,-71.56671, 15.27297,-71.35749, 16.06896,-71.18409, 18.05569,-71.21087, 18.18693,-71.56017, 17.94095,-71.79691, 18.2426,-72.26287, 18.1768,-72.51207, 18.6428,-72.84655, 18.41702,-73.21914, 17.21973,-73.51087, 15.95491,-73.67205, 14.53724,-73.81775, 13.29543,-74.21443, 11.09112,-74.30085, 9.31902,-74.1996, 8.07684,-73.94694, 7.24562,-73.75243, 7.14798,-73.24678, 7.86999,-72.77294, 8.78747,-72.39554, 10.11832,-72.2524, 11.27343,-72.09003, 12.32697,-71.83623))=1\norder by t_min\n\n-- Search for data cubes whose footprint (s_region)\n-- intersects a polygon, around the Small Magellanic Cloud;\n-- show only the identifier of the product, the instrument name, the min and max wavelengths,\n-- the start and stop times, and the limiting magnitude (AB).\n-- Sort by start time.\n-- In the SELECT part, add s_region if you want to display the footprints, e.g., when querying using a tool;\n-- add access_url if you want to access the product files (or related files).",
            "-- Use case name: 'obs wavelength_coverage 1'\n\nSELECT TOP 200 target_name, instrument_name, dataproduct_type,\n   em_min, em_max, em_res_power, obs_collection, dataproduct_subtype \nFROM ivoa.ObsCore \nWHERE em_min <= 2.18E-6 \n  AND em_max >= 2.18E-6 \n  AND s_dec < -74.5\n-- and em_res_power < 10 -- to exclude narrow-band filters like the Hawki-BrGamma\nORDER BY s_dec\n-- \n-- Find products containing the 2.18 micron wavelength,\n-- which are closer than 15.5 degrees to the south pole (FK5 J2000).\n-- Limit the results to the first 200 records.\n-- Expected results:\n--   . Any spectrum, cube, or visibility, whose wavelengths extend below and above the 2.18 micron\n--   . Any image, source table, catalog, whose filter covers the 2.18 micron wavelength\n--     (i.e. the Ks-filter of all instruments, the BrGamma narrow-band filter of HAWKY), \n--     (see Helper: Characteristics of ESO filters below)\n--   . Only products whose declination is southern than -74.5 degrees (FK5 J2000) are considered.\n--   . ORDER BY: the results will be sorted by the declination in the default ascending order.\n--   . TOP 200: only the first 200 records (at maximum) of the result set will be returned,\n--     even if more results are available.\n--   . The TOP operator is applied to the query result set, that is, after the sorting has taken place;\n--     effectively, this means that only the first 200 closest products to the south pole are returned.",
            "-- Use case name: 'obs wavelength_coverage 2'\n\n\nSELECT TOP 200 target_name, instrument_name, em_min, em_max, em_res_power, obs_id, obs_publisher_did \nFROM ivoa.Obscore \nWHERE dataproduct_type = 'spectrum'                 -- find 1d spectra with \n  AND em_min < 656.28E-9 AND em_max > 656.28E-9     -- wavelength covering the Halpha wavelength (in meters, at redshift 0.0)\n\n-- Find 200 1d spectra covering the Halpha wavelength (in meters, at redshift 0.0)\n\n-- The list of standard dataproduct_type values supported by the ESO archive are:\n-- 'spectrum', 'image', 'cube', 'measurements', 'visibility'\n-- Some ESO-specific types are provided via the dataproduct_subtype column.",
            "-- Use case name: 'obs spatial_query_footprint 1'\n\nSELECT target_name, s_dec, s_ra, t_exptime, em_res_power, em_min, dataproduct_type, instrument_name,   \n       3600.*distance(s_region, point('',161.265,-59.684)) as arcsec_from_eta_car   \nFROM   ivoa.Obscore    \nWHERE  CONTAINS(s_region,CIRCLE('ICRS',161.265, -59.684, 100./3600.)) = 1   -- a circle centered onto Eta Car, of radius=100 arcsec,\n                                                                           -- must contain the s_region\n                                                                           -- (the footprint of the ESO 1d spectra is always a point!)\nand em_res_power > 27000                                     -- R (resolving power) > 27 000 \norder by arcsec_from_eta_car                                 -- sorted (ascending) on the computed field called arcsec_from_eta_car \n\n-- Find products whose footprints are enterily within a cone of radius 100 arcsec around Eta Carinae\n-- with a spectral resolving power of at least 27,000.\n-- For the 'the-cone-fully-contains-product' search, the CONTAINS(contained, container) construct is used,\n-- that is, the 'contained' must entirely lie within the 'container'  \n-- Refer and compare with ospfoot2.",
            "-- Use case name: 'obs spatial_query_footprint 2'\n\n-- with a spectral resolving power of at least 27,000.\n-- For the 'product-intersecting-user-defined-cone' search, the INTERSECTS(shape, shape) construct is used,\n-- that is, the two shapes need to overlap, but not necessarily 100 percent. \n\nSELECT target_name, s_dec, s_ra, t_exptime, em_res_power, em_min*1E9 as em_min, dataproduct_type, instrument_name,   \n       3600.*distance(s_region, point('',83.86675,-69.269741666)) as arcsec_from_sn1987a   \nFROM   ivoa.Obscore    \nWHERE  INTERSECTS(s_region, CIRCLE('ICRS',83.86675,-69.269741666, 100./3600.)) = 1\n                           -- a circle centered onto SN1987A, of radius=100 arcsec,\n                                     -- must intersect the footprint of the product (field: s_region)\n                                     -- (Note that the footprint of a ESO 1d spectra is always a point)\nand em_res_power > 27000             -- R (resolving power) > 27 000 \norder by arcsec_from_sn1987a         -- sorted (ascending) on the computed field called arcsec_from_sn1987a \n\n-- Find products whose footprints intersect a cone of radius 100 arcsec around SN1987A\n-- with a spectral resolving power of at least 27,000.\n-- For the 'the-cone-intersects-product' search, the INTERSECTS(region1� region2) construct is used,\n-- the order of the two arguments is irrelevant.\n-- Refer and compare with ospfoot1.",
            "-- Use case name: 'obs spatial_query_footprint 3'\n\nSELECT TOP 100 * FROM ivoa.Obscore \nWHERE CONTAINS(POINT('ICRS',123.2675,-34.5785),s_region) = 1  -- footprint(the s_region field) must contain point (RA,DEC)=(123.2675deg,-34.5785deg)\n\n-- Find products containing the position of the cepheid RS PUPPIS\n-- Please note that this query very likely will NOT return any spectrum. \n-- That is because, in the ESO case, 1D spectra have point-like footprints. \n-- Refer and compare with ospfoot4.",
            "-- Use case name: 'obs spatial_query_footprint 4'\n\nSELECT target_name, s_ra, s_dec, dataproduct_type, distance(s_region, POINT('',123.2675,-34.5785))\nFROM ivoa.Obscore \nWHERE INTERSECTS(CIRCLE('ICRS',123.2675,-34.5785,60./3600.),s_region) = 1  -- footprint of product (s_region) must overlap a 1 arcmin cone around RS PUPPIS \nORDER by 5 \n\n-- Find products intersecting a small circle around the cepheid RS PUPPIS\n-- Please note that this query will also return data products with a point-type footprint (spectra and visibilities)! \n-- Refer and compare with ospfoot3.",
            "-- Use case name: 'obs advanced 1'\n\nSELECT * FROM ivoa.ObsCore WHERE dp_id in (\n   SELECT distinct J.dp_id FROM\n      (select * FROM ivoa.Obscore WHERE dataproduct_subtype ='srctbl' \n      AND obs_collection = 'HAWKI' \n      AND gal_lat < 10 AND gal_lat > -10 \n      AND em_min < 1.265E-6 AND em_max > 1.265E-6 ) J, \n\n      (select * FROM ivoa.Obscore WHERE dataproduct_subtype ='srctbl' \n      AND obs_collection = 'HAWKI' \n      AND gal_lat < 10 AND gal_lat > -10 \n      AND em_min < 1.66E-6 AND em_max > 1.66E-6 ) H \n\n   WHERE INTERSECTS( J.s_region , H.s_region)=1 and \n   ESO_INTERSECTION( J.s_region , H.s_region) > 0.8*AREA( J.s_region ) \n)\n\n-- Find overlapping J and H HAWKI source tables within the GC plane:\n-- provided they overlap for at least 80% of the J image.\n-- \n-- ESO specific:\n--    (A) gal_lat (and gal_lon) are ESO specific columns (not part of the ObsCore standard)\n--    (B) ESO_INTERSECTION is an ESO-defined function\n--        which returns the area of the intersection between 2 regions.\n-- Note: You can dismiss the gal_lat constraints and try on the whole sky.",
            "-- Use case name: 'obs advanced 2'\n\nSELECT s_resolution as PSF_FWHM,\nAVG(turbfwhm) as mass_seeing_avg, AVG(tet) as mass_isoplanaticangle_avg,\ncount(distinct(mass.start_date)) as mass_num_samples,\nmin(mass.start_date) as mass_start_date, max(mass.start_date) as mass_stop_date,\nraw.dp_id as Originating_RAW\nFROM ivoa.obscore,\nasm.MASS_paranal as mass,\ndbo.raw as raw\nWHERE mass.start_date  between eso_dateadd_sec(-360, raw.exp_start) and eso_dateadd_sec(360, raw.exp_start)\nand instrument='HAWKI'\nand raw.dp_id in\n      (select source_file from phase3v2.provenance as p, phase3v2.files as f\n       where p.product_file=f.file_id and archive_id=ivoa.obscore.dp_id)\nand ivoa.obscore.dp_id='ADP.2016-07-29T13:44:04.217'\ngroup by s_resolution, obscore.dp_id, raw.dp_id\n-- The results do not take into account the scaling necessary\n-- to bring the seeing to the airmass and wavelength of the observation.\n-- See: http://www.eso.org/observing/etc/doc/helpfors.html#seeing\n-- Other bands:\n-- ADP.2016-07-29T13:44:03.709 J   (FWHM_J / FWHM_V = 0.85 * POW(AIRMASS_OBS / AIRMASS_MASS, 0.6) )\n-- ADP.2016-07-18T09:00:02.473 Brackett Gamma\n-- ADP.2016-07-29T13:44:04.217 Ks  (FWHM_Ks / FWHM_V = 0.76 * POW(AIRMASS_OBS / AIRMASS_MASS, 0.6))",
            "SELECT * from ivoa.ObsCore\nWHERE dp_id='ADP.2014-10-01T10:19:21.580'\n\n-- you could use, completely equivalently, its ESO VO identifier using:\n-- WHERE obs_publisher_did='ivo://eso.org/ID?ADP.2014-10-01T10:19:21.580'",
            "SELECT * FROM ivoa.Obscore \nWHERE dataproduct_type='image' \nAND s_resolution < 0.4 \nAND s_ra BETWEEN 240 AND 255 AND s_dec BETWEEN -10 AND 0     -- RA in range: 240..255, DEC in range: -10..0\nAND ((em_min < 2.2E-6 AND em_max > 2.2E-6)                   -- wavelength covering (either the Ks band\n    OR (em_min < 1.65E-6 AND em_max > 1.65E-6)               --                          or the H band,\n    OR (em_min < 1.25E-6 AND em_max > 1.25E-6))              --                          or the J band).",
            "SELECT TOP 10 obs_publisher_did, instrument_name, em_min, em_max, em_res_power, obs_collection \nFROM ivoa.ObsCore \nWHERE em_min < 2.159E-6 AND em_max > 2.159E-6 -- contains the central lambda of Ks of the 2MASS-Cohen standard photometric system\nAND em_res_power between 5 and 10 -- and limits the resolving power to a small value (no spectrum/cube will match)\n                                  -- Expected are em_res_power(Ks)=6.22 of HAWKI, 7.1 of VIRCAM, 8.0 of ISAAC,\n                                  -- but not the BrGamma of HAWKI (69.85).\n                                  -- See the Helper tables below.",
            "SELECT COUNT(*) FROM ivoa.ObsCore",
            "SELECT COUNT(*) as qty, obs_collection   -- Num of datasets in collection\nFROM ivoa.ObsCore \nGROUP BY obs_collection           -- group on the field(s) you want to compute the aggregates\nORDER BY qty",
            "SELECT COUNT(*), obs_collection, dataproduct_type   -- Num of datasets per collection and data product type\nFROM ivoa.ObsCore \nGROUP BY obs_collection, dataproduct_type           -- group on the field(s) you want to compute the aggregates\nORDER BY obs_collection, dataproduct_type           -- you might want to order by COUNT(*) instead",
            "SELECT min(em_res_power) as min_res_power,    -- Find min of the resolving \n       max(em_res_power) as max_res_power,    --  and max of the resolving power \n       dataproduct_type                       --  for a data product type\nFROM ivoa.ObsCore \nGROUP BY dataproduct_type                     -- group on the field(s) you want to compute the aggregates",
            "SELECT min(em_res_power) as min_res_power,  max(em_res_power) as max_res_power, \nobs_collection \nFROM ivoa.ObsCore \nGROUP BY obs_collection",
            "SELECT count(*) as qty, obs_collection, dataproduct_type, dataproduct_subtype, \ninstrument_name, filter, min(em_min*1E9) as wmin_nm, max(em_max*1E9) as wmax_nm, \nmin(snr) as min_snr, max(snr) as max_snr, min(abmaglim) as min_mag_lim, \nmax(abmaglim) as max_mag_lim\n FROM ivoa.ObsCore\n group by obs_collection, dataproduct_type, dataproduct_subtype, facility_name, instrument_name, filter\n order by obs_collection, dataproduct_type, dataproduct_subtype, instrument_name, filter",
            "SELECT dataproduct_type, count(*) NumProducts, sum(access_estsize)/1000000000. TB \nFROM ivoa.ObsCore group by dataproduct_type \norder by 3 desc",
            "SELECT TOP 100 instrument_name, em_min, em_max, s_resolution, distance(s_region, point('',266.42,-29.0)) as dist_from_GC, dataproduct_type \n FROM ivoa.Obscore \nWHERE dataproduct_type='image' \n AND INTERSECTS(CIRCLE('ICRS',266.42,-29.0,5),s_region) = 1  -- intersecting (aka overlapping) a 5 deg cone around the Galactic Centre\n AND ((em_min < 1.25E-6 AND em_max > 1.25E-6)   -- Contains lambda(J) or\n     OR (em_min < 1.65E-6 AND em_max > 1.65E-6) -- Contains lambda(H) or\n     OR (em_min < 2.2E-6 AND em_max > 2.2E-6))  -- Contains lambda(Ks) \n AND em_res_power < 20                          -- Low spectral resolving power, includes both images and source tables \n ORDER BY 4               -- 4 is the position of the s_resolution within fields in the SELECT",
            "SELECT TOP 50 instrument_name, em_min, em_max, s_resolution, distance(s_region, point('',266.42,-29.0)) as dist_footprint_from_GC \n FROM ivoa.Obscore \nWHERE dataproduct_type='image' \n AND INTERSECTS(CIRCLE('ICRS',266.42,-29.0,5),s_region) = 1 \n AND ((em_min < 1.265E-6 AND em_max > 1.265E-6) -- Contains lambda(J) \n     OR (em_min < 1.66E-6 AND em_max > 1.66E-6) -- Contains lambda(H) \n     OR (em_min < 2.18E-6 AND em_max > 2.18E-6)) -- Contains lambda(Ks) \n AND dataproduct_type = 'image' \n ORDER BY 5              -- 5 is the position in the SELECT of the distance from \n                         -- the galactic center of the returned footprint (aliased to dist_footprint_from_GC), in degrees)",
            "SELECT TOP 50 J.target_name, J.obs_publisher_did, J.s_ra, J.s_dec, J.em_min, J.em_max, J.instrument_name,\n              H.target_name, H.obs_publisher_did, H.s_ra, H.s_dec, H.em_min, H.em_max, H.instrument_name,\n              ESO_INTERSECTION(J.s_region,H.s_region) as OverlappingArea, J.s_region, H.s_region\nFROM\n     (SELECT * FROM ivoa.Obscore WHERE dataproduct_type='image'\n        AND INTERSECTS(CIRCLE('ICRS',266.42,-29.0,5),s_region) = 1\n        AND em_min < 1.265E-6 AND em_max > 1.265E-6\n     ) as J\n,\n     (SELECT * FROM ivoa.Obscore WHERE dataproduct_type='image'\n        AND INTERSECTS(CIRCLE('ICRS',266.42,-29.0,5),s_region) = 1\n        AND em_min < 1.66E-6 AND em_max > 1.66E-6\n     ) as H\nWHERE INTERSECTS(J.s_region, H.s_region)=1\n  AND ESO_INTERSECTION(J.s_region,H.s_region) > 1.79\nORDER by 1, OverlappingArea desc",
            "SELECT *\n       FROM\n             (select * FROM ivoa.Obscore WHERE dataproduct_type ='image'\n             AND INTERSECTS(CIRCLE('ICRS',266.42,-29.0,5), s_region) = 1 \n             AND em_min < 1.265E-6 AND em_max > 1.265E-6 ) J, \n\n            (select * FROM ivoa.Obscore WHERE dataproduct_type ='image' \n             AND INTERSECTS(CIRCLE('ICRS',266.42,-29.0,5), s_region) = 1 \n             AND em_min < 1.66E-6 AND em_max > 1.66E-6 ) H \n\n       WHERE INTERSECTS( J.s_region , H.s_region)=1 and \n       ESO_INTERSECTION( J.s_region , H.s_region) > 1.79",
            "SELECT  TOP 1000 *\nFROM ivoa.Obscore\nWHERE dataproduct_type='image'\n AND INTERSECTS(CIRCLE('ICRS',266.42,-29.0,5), s_region) = 1\n AND em_min < 1.265E-6 AND em_max > 1.265E-6 -- Contains lambda(J)",
            "SELECT top 120\n   distance(s_region, point('',266.42,-29.0)) as dist_footprint, -- CORRECT\n   distance(point('',s_ra,s_dec),point('',266.42,-29.0)) as dist_ra_dec, -- INCORRECT\ndataproduct_type, s_ra, s_dec\nFROM ivoa.Obscore \nWHERE dataproduct_type='image' \nAND INTERSECTS(CIRCLE('ICRS',266.42,-29.0,5),s_region) = 1  -- intersecting (aka overlapping) a 5 deg cone around the Galactic Centre\nAND em_min < 1.25E-6 AND em_max > 1.25E-6   -- Contains lambda(J)\nORDER BY dist_footprint, dist_ra_dec\n-- \n-- CAUTION: Never use s_ra, s_dec when computing distances, use s_region instead!",
            "SELECT spectrum.* FROM\n      (select * FROM ivoa.Obscore WHERE dataproduct_type='spectrum' and snr > 500) spectrum,\n\n      (select * FROM ivoa.Obscore WHERE dataproduct_subtype ='tile'\n      AND obs_collection = 'VVV'  AND em_min < 1.66E-6 AND em_max > 1.66E-6 ) VVV_H\n\nWHERE CONTAINS( spectrum.s_region , VVV_H.s_region)=1\n\n-- CAUTION: if you use collection/band with many more records than VVV/H,\n--          then the query might take longer time: use /async in that case.",
            "SELECT spectrum.* FROM\n      (select * FROM ivoa.Obscore WHERE dataproduct_type='spectrum' and snr > 500) spectrum,\n\n      (select * FROM ivoa.Obscore WHERE dataproduct_subtype ='tile'\n      AND obs_collection = 'VVV'  AND filter = 'H' ) VVV_H\n\nWHERE CONTAINS( spectrum.s_region , VVV_H.s_region)=1\n\n-- It is the same query as the one before, but this time using\n-- the ESO-specific field called filter, not part of the ObsCore standard\n-- CAUTION: if you use collection/band with many more records than VVV/H,\n--          then the query might take longer time: use /async in that case.",
            "SELECT count(*) as qty, spectrum.instrument_name, spectrum.em_res_power,\n               min(spectrum.snr) as min_snr, max(spectrum.snr) as max_snr\nFROM\n      (select * FROM ivoa.Obscore WHERE dataproduct_type='spectrum' and snr > 500) spectrum,\n \n      (select * FROM ivoa.Obscore WHERE dataproduct_subtype ='tile'\n      AND obs_collection = 'VVV'  AND em_min < 1.66E-6 AND em_max > 1.66E-6 ) VVV_H\n \nWHERE CONTAINS( spectrum.s_region , VVV_H.s_region)=1\nGROUP BY spectrum.instrument_name, spectrum.em_res_power\n\n-- Use text mode to better visualise these results.",
            "SELECT * from dbo.raw\nwhere prog_id='60.A-9022(C)'\nand instrument in ('SHOOT', 'XSHOOTER')\nand dp_cat='SCIENCE'\nand dp_tech like 'ECHELLE%'\n\n-- Please note that the XSHOOTER instrument, during its life time,\n-- has changed name. This is to be taken into account when querying\n-- the raw data database: to be sure to get all the XSHOOTER data\n-- always use both possible names with the constraint: \n--    instrument in ('SHOOT','XSHOOTER')",
            "SELECT start_date, midpoint_date, ra, dec, alt, az, *\nFROM asm.lhatpros_paranal_profiles\nWHERE midpoint_date between eso_dateadd_sec(-2700.000000, '2019-06-06T09:55:27') and eso_dateadd_sec(2700.000000, '2019-06-06T09:55:27')\n  AND CONTAINS(POINT('',ra, dec), CIRCLE('', 332.200000, -25.500000, 2.500000))=1\n  AND valid=1",
            "SELECT source_file from phase3v2.provenance as p, phase3v2.files as f\nwhere p.product_file=f.file_id and archive_id='ADP.2017-10-25T22:38:07.225'\n\n-- This is a UVES 1d-spectrum product, generated out of a single raw files.\n-- The query in this case returns that raw file.",
            "SELECT source_file from phase3v2.provenance as p, phase3v2.files as f\nwhere p.product_file=f.file_id and archive_id='ADP.2016-07-29T13:44:04.217'\n\n-- This is a HARPS product, generated out of some 42 HARPS raw files.\n-- The query in this case returns the list of those 42 raw files.",
            "-- Given a product identifier: ADP.2015-05-11T10:20:22.733\n-- find out from which file(s) it was generated:\n\nSELECT this_product, dataproduct_type, originated_from, originating_product.type \"type (if product)\", originating_raw.type \"type (if raw)\"\nFROM \n\n  (SELECT dp_id, dataproduct_type FROM ivoa.ObsCore) as obscore,\n\n  (SELECT archive_id as this_product, source_file as originated_from\n   FROM phase3v2.provenance as p, phase3v2.files as f\n   WHERE p.product_file=f.file_id) as provenance\n\n  LEFT JOIN (SELECT dp_id, dataproduct_type as type FROM ivoa.ObsCore) as originating_product ON provenance.originated_from = originating_product.dp_id\n\n  LEFT JOIN (SELECT dp_id, dp_cat || dp_tech || dp_type as type FROM dbo.raw) as originating_raw ON provenance.originated_from = originating_raw.dp_id\n\nWHERE provenance.this_product = obscore.dp_id\nAND obscore.dp_id='ADP.2015-05-11T10:20:22.733'",
            "Select count(*) provenanceCardinality, dp_id, obs_collection, dataproduct_type, dataproduct_subtype\nfrom (select dp_id, obs_collection, dataproduct_type, dataproduct_subtype from ivoa.ObsCore) allobs,\n        (SELECT archive_id, source_file from phase3v2.provenance as p, phase3v2.files as f\n         where p.product_file=f.file_id) prov\nwhere  allobs.dp_id=prov.archive_id\ngroup by dp_id,obs_collection, dataproduct_type, dataproduct_subtype\nhaving count(*) > 400\norder by 1 desc, 4",
            "SELECT source_file,\n       archive_id as product_id,\n       category   as product_category,\n       name       as product_origfile\nfrom phase3v2.provenance as p, phase3v2.files as f\nwhere p.product_file=f.file_id and source_file='HAWKI.2015-10-01T09:14:56.450'",
            "SELECT top 1 \nPOLYGON('', 0,80, 270,80, 180,80, 90,80) as myPolygon,\narea(POLYGON('', 0,80, 270,80, 180,80, 90,80)) as myArea,\ncontains( POINT('', 0, 90), POLYGON('', 0,80, 270,80, 180,80, 90,80) ) as isNorthPoleInside,\ncontains( POINT('', 0, -90), POLYGON('', 0,80, 270,80, 180,80, 90,80) ) as isSouthPoleInside\nFROM ivoa.ObsCore",
            "SELECT top 1 \nPOLYGON('', 0,-80, 90,-80, 180,-80, 270,-80) as myPolygon,\narea(POLYGON('', 0,-80, 90,-80, 180,-80, 270,-80)) as myArea,\ncontains( POINT('', 0, 90), POLYGON('', 0,-80, 90,-80, 180,-80, 270,-80) ) as isNorthPoleInside,\ncontains( POINT('', 0, -90), POLYGON('', 0,-80, 90,-80, 180,-80, 270,-80) ) as isSouthPoleInside\nFROM ivoa.ObsCore",
        ];

        struct TestCase<'a> {
            query: &'a str,
            description: &'a str,
            version: &'a str,
            valid: bool,
        }

        struct TestFailure {
            msg: String,
            query: String,
        }

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

        fn get_test_cases<'a>(doc: &'a Document) -> Vec<TestCase<'a>> {
            doc.descendants()
                .filter(|node| node.has_tag_name("query"))
                .map(|node| {
                    let description = node
                        .descendants()
                        .find(|node| node.has_tag_name("description"))
                        .expect("Missing description element")
                        .text()
                        .expect("Missing description text");
                    let adql_node = node
                        .descendants()
                        .find(|node| node.has_tag_name("adql"))
                        .expect("Missing adql element");
                    let query = adql_node.text().expect("Missing adql text").trim();
                    let version = adql_node
                        .attribute("version")
                        .expect("Missing version attribute");
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

        async fn test_single_xml(
            ctx: &SessionContext,
            path: &Path,
        ) -> Vec<Result<(), TestFailure>> {
            let text = std::fs::read_to_string(path).expect("Failed to read Lyonetia XML file");
            let doc = roxmltree::Document::parse(&text).expect("Failed to parse Lyonetia XML file");
            let test_cases = get_test_cases(&doc);

            stream::iter(test_cases).then(async |case| {
            let plan = adql_query_to_plan(ctx, case.query).await;
            match (plan, case.valid) {
                // pass, valid as expected
                (Ok(_), true) => Ok(()),
                // pass, invalid as expected
                (Err(_), false) => Ok(()),
                // fail, we mark as invalid, but ADQL query satisfies the standard
                (Err(err), true) => {
                    Err(TestFailure {
                        query: case.query.to_string(),
                        msg: format!("Expected valid ADQL query, but failed to parse: {}\nQuery: {}\nDescription: {}\nVersion: {}\nFile: {:?}", err, case.query, case.description, case.version, path)
                    })
                }
                // fail, we mark as valid, but ADQL query doesn't satisfy the standard
                (Ok(_), false) => {
                    Err(TestFailure {
                        query: case.query.to_string(),
                        msg: format!("Expected invalid ADQL query, but parsed successfully:\nQuery: {}\nDescription: {}\nVersion: {}\nFile: {:?}", case.query, case.description, case.version, path)
                })}
            }
        }).collect().await
        }

        fn test_report(results: Vec<Result<(), TestFailure>>) {
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
                .filter(|failure| !KNOWN_TO_FAIL.contains(&failure.query.as_str()))
                .collect();
            if !unexpected_failures.is_empty() {
                let report = unexpected_failures
                    .iter()
                    .map(|failure| failure.msg.as_str())
                    .collect::<Vec<_>>()
                    .join("\n\n");
                println!("Unexpected test failures:\n\n{}", report);
                let queries = unexpected_failures
                    .iter()
                    .map(|failure| failure.query.as_str())
                    .collect::<Vec<_>>();
                panic!(
                    "{} of total {} tests failed unexpectedly, see the full report with `cargo test lyonetia -- --nocapture`: {:#?}",
                    queries.len(),
                    n_total,
                    queries
                );
            }
        }

        #[tokio::test]
        async fn test() {
            let ctx = SessionContext::new();
            register_adql(&ctx);

            let xml_paths = get_xml_paths();
            let results = stream::iter(xml_paths)
                .then(async |path| test_single_xml(&ctx, &path).await)
                .flat_map(stream::iter)
                .collect::<Vec<_>>()
                .await;

            test_report(results);
        }
    }
}
