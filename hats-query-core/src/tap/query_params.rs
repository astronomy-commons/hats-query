//! Parameters of TAP queries, see TAP 1.1 section 2.7 and DALI 1.1 section 3.

use crate::adql::is_regular_identifier;
use mime::Mime;
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::str::FromStr;
use url::Url;

/// Checked parameters of a TAP query, the same for sync and async jobs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TapQueryParams {
    /// Language of the query text.
    lang: QueryLanguage,
    /// Query text, not blank.
    query: String,
    /// Requested output format.
    response_format: ResponseFormat,
    /// Row limit requested by the client, 0 asks for the metadata only.
    maxrec: Option<u64>,
    /// Client's label of the job, to be returned with the results.
    runid: Option<String>,
    /// Tables uploaded by the client, keyed by their lower-case names.
    uploads: BTreeMap<String, Upload>,
}

impl TapQueryParams {
    /// Parses the request parameters `pairs`, given as (name, value).
    ///
    /// Parameter names are case-insensitive, values are case-sensitive. Unknown parameters are
    /// ignored, including REQUEST and VERSION removed in TAP 1.1. Returns an error if a required
    /// parameter is missing, a single-valued parameter is given more than once, or a value is
    /// invalid.
    pub fn from_pairs<K, V>(
        pairs: impl IntoIterator<Item = (K, V)>,
    ) -> Result<Self, TapQueryParamsError>
    where
        K: AsRef<str>,
        V: AsRef<str>,
    {
        let mut builder = TapQueryParamsBuilder::default();
        for (name, value) in pairs {
            builder.param(name.as_ref(), value.as_ref())?;
        }
        builder.build()
    }

    /// Language of the query text.
    pub fn lang(&self) -> QueryLanguage {
        self.lang
    }

    /// Query text.
    pub fn query(&self) -> &str {
        &self.query
    }

    /// Requested output format.
    pub fn response_format(&self) -> ResponseFormat {
        self.response_format
    }

    /// Row limit requested by the client, 0 asks for the metadata only, [None] if not given.
    pub fn maxrec(&self) -> Option<u64> {
        self.maxrec
    }

    /// Client's label of the job, [None] if not given.
    pub fn runid(&self) -> Option<&str> {
        self.runid.as_deref()
    }

    /// Tables uploaded by the client, ordered by their lower-case names.
    pub fn uploads(&self) -> impl Iterator<Item = &Upload> {
        self.uploads.values()
    }
}

/// Collector of TAP query parameters, checking each one as it is added.
#[derive(Debug, Default)]
struct TapQueryParamsBuilder {
    /// Language of the query text.
    lang: Option<QueryLanguage>,
    /// Query text, not blank.
    query: Option<String>,
    /// Requested output format.
    response_format: Option<ResponseFormat>,
    /// Row limit requested by the client.
    maxrec: Option<u64>,
    /// Client's label of the job.
    runid: Option<String>,
    /// Tables uploaded by the client, keyed by their lower-case names.
    uploads: BTreeMap<String, Upload>,
}

impl TapQueryParamsBuilder {
    /// Name of the output format parameter and its TAP 1.0 synonym.
    const FORMAT_NAMES: &str = "RESPONSEFORMAT (or its synonym FORMAT)";

    /// Adds the request parameter `name` with `value`, ignores unknown parameters.
    ///
    /// An UPLOAD value may list several tables separated by ";", as TAP 1.1 section 2.7.6 allows.
    /// Returns an error if the value is invalid, or the single-valued parameter is already set.
    /// Uploaded table names must differ case-insensitively.
    fn param(&mut self, name: &str, value: &str) -> Result<(), TapQueryParamsError> {
        match name.to_ascii_uppercase().as_str() {
            "LANG" => self.lang(value.parse()?),
            "QUERY" => self.query(value.to_owned()),
            // FORMAT is the TAP 1.0 name of RESPONSEFORMAT
            "RESPONSEFORMAT" | "FORMAT" => self.response_format(value.parse()?),
            "MAXREC" => {
                let maxrec = value.parse().map_err(|_| {
                    TapQueryParamsError::invalid("MAXREC", value, "a non-negative integer")
                })?;
                self.maxrec(maxrec)
            }
            "RUNID" => self.runid(value.to_owned()),
            "UPLOAD" => value
                .split(';')
                .try_for_each(|upload| self.upload(upload.parse()?)),
            _ => Ok(()),
        }
    }

    /// Sets the query language.
    ///
    /// Returns an error if it is already set.
    fn lang(&mut self, lang: QueryLanguage) -> Result<(), TapQueryParamsError> {
        set_once(&mut self.lang, "LANG", lang)
    }

    /// Sets the query text.
    ///
    /// Returns an error if it is blank or already set.
    fn query(&mut self, query: String) -> Result<(), TapQueryParamsError> {
        if query.trim().is_empty() {
            return Err(TapQueryParamsError::invalid(
                "QUERY",
                &query,
                "a non-empty query",
            ));
        }
        set_once(&mut self.query, "QUERY", query)
    }

    /// Sets the output format.
    ///
    /// Returns an error if it is already set.
    fn response_format(
        &mut self,
        response_format: ResponseFormat,
    ) -> Result<(), TapQueryParamsError> {
        set_once(
            &mut self.response_format,
            Self::FORMAT_NAMES,
            response_format,
        )
    }

    /// Sets the row limit, 0 asks for the metadata only.
    ///
    /// Returns an error if it is already set.
    fn maxrec(&mut self, maxrec: u64) -> Result<(), TapQueryParamsError> {
        set_once(&mut self.maxrec, "MAXREC", maxrec)
    }

    /// Sets the client's label of the job.
    ///
    /// Returns an error if it is longer than 64 characters, the DALI 1.1 section 3.4.6 limit, or
    /// already set.
    fn runid(&mut self, runid: String) -> Result<(), TapQueryParamsError> {
        if runid.chars().count() > 64 {
            return Err(TapQueryParamsError::invalid(
                "RUNID",
                &runid,
                "at most 64 characters",
            ));
        }
        set_once(&mut self.runid, "RUNID", runid)
    }

    /// Adds a table uploaded by the client.
    ///
    /// Returns an error if a table with the same case-insensitive name is already added.
    fn upload(&mut self, upload: Upload) -> Result<(), TapQueryParamsError> {
        match self.uploads.entry(upload.table_name.to_ascii_lowercase()) {
            Entry::Occupied(_) => Err(TapQueryParamsError::DuplicateUpload(upload.table_name)),
            Entry::Vacant(entry) => {
                entry.insert(upload);
                Ok(())
            }
        }
    }

    /// Checked parameters, VOTable output by default.
    ///
    /// Returns an error if the query language or text is missing.
    fn build(self) -> Result<TapQueryParams, TapQueryParamsError> {
        Ok(TapQueryParams {
            lang: self.lang.ok_or(TapQueryParamsError::Missing("LANG"))?,
            query: self.query.ok_or(TapQueryParamsError::Missing("QUERY"))?,
            response_format: self.response_format.unwrap_or_default(),
            maxrec: self.maxrec,
            runid: self.runid,
            uploads: self.uploads,
        })
    }
}

/// Language of a query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryLanguage {
    /// ADQL 2.1.
    Adql,
}

impl FromStr for QueryLanguage {
    type Err = TapQueryParamsError;

    /// Parses a LANG value, "ADQL" with an optional version, "ADQL-2.1".
    ///
    /// Returns an error for other languages and versions, as TAP 1.1 section 2.7.1 requires.
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "ADQL" | "ADQL-2.1" => Ok(Self::Adql),
            _ => Err(TapQueryParamsError::invalid(
                "LANG",
                value,
                "ADQL or ADQL-2.1",
            )),
        }
    }
}

/// Output format of query results.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ResponseFormat {
    /// VOTable with the BINARY2 serialization.
    #[default]
    VotableBinary2,
}

impl ResponseFormat {
    /// Whether the MIME type `value` is VOTable, with the BINARY2 serialization and UTF-8
    /// encoding if given.
    fn is_votable_binary2_mime(value: &str) -> bool {
        value.parse::<Mime>().is_ok_and(|mime| {
            ["application/x-votable+xml", "text/xml"].contains(&mime.essence_str())
                && mime.params().all(|(name, value)| {
                    (name == "serialization" && value == "BINARY2")
                        || (name == mime::CHARSET && value == mime::UTF_8)
                })
        })
    }
}

impl FromStr for ResponseFormat {
    type Err = TapQueryParamsError;

    /// Parses a RESPONSEFORMAT value: the VOTable short name or MIME types from DALI 1.1 section
    /// 3.4.3.
    ///
    /// MIME types and their parameter names are case-insensitive, the only allowed parameters are
    /// "serialization=BINARY2" and "charset=UTF-8".
    /// Returns an error for other values, including the formats not implemented yet.
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value == "votable" || Self::is_votable_binary2_mime(value) {
            Ok(Self::VotableBinary2)
        } else {
            Err(TapQueryParamsError::invalid(
                "RESPONSEFORMAT",
                value,
                "votable, application/x-votable+xml or text/xml, the MIME types may only have \
                the serialization=BINARY2 and charset=UTF-8 parameters",
            ))
        }
    }
}

/// Table uploaded for a query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Upload {
    /// Name to refer to it in the query as TAP_UPLOAD.<name>.
    table_name: String,
    /// Where to get the table from.
    source: UploadSource,
}

impl Upload {
    /// Name to refer to the table in the query as TAP_UPLOAD.<name>.
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Where to get the table from.
    pub fn source(&self) -> &UploadSource {
        &self.source
    }
}

impl FromStr for Upload {
    type Err = TapQueryParamsError;

    /// Parses an UPLOAD value, "<table name>,<URI>".
    ///
    /// Returns an error if the table name isn't an ADQL regular identifier, as TAP 1.1 section
    /// 2.7.6 requires, or the URI is malformed.
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value
            .split_once(',')
            .and_then(|(table_name, uri)| {
                Some(Self {
                    table_name: is_regular_identifier(table_name).then(|| table_name.to_owned())?,
                    source: UploadSource::from_uri(uri)?,
                })
            })
            .ok_or_else(|| {
                TapQueryParamsError::invalid(
                    "UPLOAD",
                    value,
                    "<table name>,<URI>, where the table name is an ADQL regular identifier: \
                    Latin letters, digits and underscores, starting with a letter, not a reserved \
                    keyword",
                )
            })
    }
}

/// Location of an uploaded table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UploadSource {
    /// URL of a scheme other than param and vos, e.g. "https://example.com/t.xml".
    Url(Url),
    /// Part of the multipart/form-data request, by its name: "param:<name>".
    Inline(String),
    /// VOSpace URI, "vos:<path>".
    VoSpace(String),
}

impl UploadSource {
    /// Table location given by `uri`, [None] if it is malformed.
    fn from_uri(uri: &str) -> Option<Self> {
        let (scheme, rest) = uri.split_once(':')?;
        let is_scheme = |name: &str| scheme.eq_ignore_ascii_case(name);
        if is_scheme("param") {
            (!rest.is_empty()).then(|| Self::Inline(rest.to_owned()))
        } else if is_scheme("vos") {
            (!rest.is_empty()).then(|| Self::VoSpace(uri.to_owned()))
        } else if uri.trim() == uri {
            Url::parse(uri).ok().map(Self::Url)
        } else {
            None
        }
    }
}

/// Stores `value` of the parameter `name` into the empty `slot`.
///
/// Returns an error if the slot is already filled: DALI 1.1 section 3.2 requires an error for a
/// single-valued parameter given more than once.
fn set_once<T>(
    slot: &mut Option<T>,
    name: &'static str,
    value: T,
) -> Result<(), TapQueryParamsError> {
    if slot.replace(value).is_some() {
        return Err(TapQueryParamsError::Repeated(name));
    }
    Ok(())
}

/// Invalid set of TAP query parameters.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TapQueryParamsError {
    /// Absent required parameter, with its name.
    #[error("Parameter {0} is required")]
    Missing(&'static str),
    /// Single-valued parameter given more than once, with its name.
    #[error("Parameter {0} is given more than once, but takes a single value")]
    Repeated(&'static str),
    /// Parameter value outside the allowed ones.
    #[error("Parameter {name} has invalid value '{value}', expected {expected}")]
    Invalid {
        /// Parameter name.
        name: &'static str,
        /// Value as given.
        value: String,
        /// Description of allowed values.
        expected: &'static str,
    },
    /// Uploaded table name given by more than one UPLOAD, case-insensitively.
    #[error(
        "Table name '{0}' is given by more than one UPLOAD parameter, table names must differ \
        case-insensitively"
    )]
    DuplicateUpload(String),
}

impl TapQueryParamsError {
    /// Invalid `value` of the parameter `name`, which should be `expected`.
    fn invalid(name: &'static str, value: &str, expected: &'static str) -> Self {
        Self::Invalid {
            name,
            value: value.to_owned(),
            expected,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(pairs: &[(&str, &str)]) -> Result<TapQueryParams, TapQueryParamsError> {
        TapQueryParams::from_pairs(pairs.iter().copied())
    }

    /// Parses the minimal valid query parameters with `extra` ones.
    fn parse_with(extra: &[(&str, &str)]) -> Result<TapQueryParams, TapQueryParamsError> {
        let mut pairs = vec![("LANG", "ADQL"), ("QUERY", "SELECT 1")];
        pairs.extend_from_slice(extra);
        parse(&pairs)
    }

    fn assert_invalid(extra: &[(&str, &str)], name: &str) {
        match parse_with(extra) {
            Err(TapQueryParamsError::Invalid { name: actual, .. }) => assert_eq!(actual, name),
            other => panic!("Expected invalid {name} for {extra:?}, got {other:?}"),
        }
    }

    #[test]
    fn minimal() {
        let params = parse_with(&[]).unwrap();
        assert_eq!(params.lang(), QueryLanguage::Adql);
        assert_eq!(params.query(), "SELECT 1");
        assert_eq!(params.response_format(), ResponseFormat::VotableBinary2);
        assert_eq!(params.maxrec(), None);
        assert_eq!(params.runid(), None);
        assert_eq!(params.uploads().count(), 0);
    }

    #[test]
    fn all_params() {
        let params = parse(&[
            ("LANG", "ADQL-2.1"),
            ("QUERY", "SELECT ra FROM TAP_UPLOAD.t"),
            ("RESPONSEFORMAT", "votable"),
            ("MAXREC", "10"),
            ("RUNID", "My Run"),
            ("UPLOAD", "t,https://example.com/t.xml"),
        ])
        .unwrap();
        assert_eq!(params.lang(), QueryLanguage::Adql);
        assert_eq!(params.query(), "SELECT ra FROM TAP_UPLOAD.t");
        assert_eq!(params.response_format(), ResponseFormat::VotableBinary2);
        assert_eq!(params.maxrec(), Some(10));
        assert_eq!(params.runid(), Some("My Run"));
        assert_eq!(params.uploads().count(), 1);
    }

    #[test]
    fn builder_setters() {
        let mut builder = TapQueryParamsBuilder::default();
        builder.lang(QueryLanguage::Adql).unwrap();
        builder.query("SELECT 1".to_owned()).unwrap();
        builder.maxrec(5).unwrap();
        builder.upload("t,param:t".parse().unwrap()).unwrap();
        let params = builder.build().unwrap();
        assert_eq!(
            params,
            parse_with(&[("MAXREC", "5"), ("UPLOAD", "t,param:t")]).unwrap()
        );
    }

    #[test]
    fn builder_rejects_repeated_setter() {
        let mut builder = TapQueryParamsBuilder::default();
        builder.maxrec(1).unwrap();
        assert_eq!(
            builder.maxrec(2).unwrap_err(),
            TapQueryParamsError::Repeated("MAXREC")
        );
    }

    #[test]
    fn names_are_case_insensitive() {
        let params = parse(&[("lang", "ADQL"), ("Query", "SELECT 1"), ("maxRec", "0")]).unwrap();
        assert_eq!(params.query(), "SELECT 1");
        assert_eq!(params.maxrec(), Some(0));
    }

    #[test]
    fn unknown_params_ignored() {
        assert!(parse_with(&[("FOO", "bar"), ("PHASE", "RUN")]).is_ok());
    }

    /// REQUEST and VERSION are removed in TAP 1.1, so ignored with any values.
    #[test]
    fn removed_params_ignored() {
        assert!(
            parse_with(&[
                ("REQUEST", "doQuery"),
                ("REQUEST", "getCapabilities"),
                ("VERSION", "1.0")
            ])
            .is_ok()
        );
    }

    #[test]
    fn missing_lang() {
        assert_eq!(
            parse(&[("QUERY", "SELECT 1")]),
            Err(TapQueryParamsError::Missing("LANG"))
        );
    }

    #[test]
    fn missing_query() {
        let error = parse(&[("LANG", "ADQL")]).unwrap_err();
        assert_eq!(error, TapQueryParamsError::Missing("QUERY"));
        assert_eq!(error.to_string(), "Parameter QUERY is required");
    }

    #[test]
    fn repeated_query() {
        let error = parse_with(&[("query", "SELECT 2")]).unwrap_err();
        assert_eq!(error, TapQueryParamsError::Repeated("QUERY"));
        assert_eq!(
            error.to_string(),
            "Parameter QUERY is given more than once, but takes a single value"
        );
    }

    #[test]
    fn format_and_responseformat_together() {
        assert_eq!(
            parse_with(&[("FORMAT", "votable"), ("RESPONSEFORMAT", "votable")]),
            Err(TapQueryParamsError::Repeated(
                "RESPONSEFORMAT (or its synonym FORMAT)"
            ))
        );
    }

    #[test]
    fn lang_values() {
        for lang in ["ADQL", "ADQL-2.1"] {
            let params = parse(&[("LANG", lang), ("QUERY", "SELECT 1")]).unwrap();
            assert_eq!(params.lang(), QueryLanguage::Adql);
        }
        for lang in ["adql", "ADQL-2.0", "ADQL-3.0", "SQL", ""] {
            let error = parse(&[("LANG", lang), ("QUERY", "SELECT 1")]).unwrap_err();
            assert!(
                matches!(error, TapQueryParamsError::Invalid { name: "LANG", .. }),
                "{lang}: {error:?}"
            );
        }
    }

    #[test]
    fn blank_query() {
        for query in ["", "  \n"] {
            let error = parse(&[("LANG", "ADQL"), ("QUERY", query)]).unwrap_err();
            assert!(matches!(
                error,
                TapQueryParamsError::Invalid { name: "QUERY", .. }
            ));
        }
    }

    #[test]
    fn response_format_values() {
        for format in [
            "votable",
            "application/x-votable+xml",
            "text/xml",
            "Application/X-VOTable+XML",
            "application/x-votable+xml;serialization=BINARY2",
            "text/xml; serialization=BINARY2",
            "Application/X-VOTable+XML; Serialization=\"BINARY2\"",
            "application/x-votable+xml;",
            "text/xml;charset=UTF-8",
            "application/x-votable+xml; charset=utf-8; serialization=BINARY2",
        ] {
            for name in ["RESPONSEFORMAT", "FORMAT", "format"] {
                let params = parse_with(&[(name, format)]).unwrap();
                assert_eq!(params.response_format(), ResponseFormat::VotableBinary2);
            }
        }
        for format in [
            "VOTable",
            "csv",
            "application/x-votable+xml;serialization=TABLEDATA",
            // MIME parameter values are case-sensitive
            "application/x-votable+xml;serialization=binary2",
            "application/x-votable+xml;content=datalink",
            "text/xml;charset=ISO-8859-1",
            " text/xml",
            "",
        ] {
            assert_invalid(&[("RESPONSEFORMAT", format)], "RESPONSEFORMAT");
        }
        // Errors name the DALI parameter, FORMAT is its equivalent
        assert_invalid(&[("FORMAT", "csv")], "RESPONSEFORMAT");
    }

    #[test]
    fn maxrec_values() {
        assert_eq!(parse_with(&[("MAXREC", "0")]).unwrap().maxrec(), Some(0));
        for maxrec in ["-1", "1.5", "ten", " 1", ""] {
            assert_invalid(&[("MAXREC", maxrec)], "MAXREC");
        }
        let error = parse_with(&[("MAXREC", "-1")]).unwrap_err();
        assert_eq!(
            error.to_string(),
            "Parameter MAXREC has invalid value '-1', expected a non-negative integer"
        );
    }

    #[test]
    fn runid_values() {
        assert_eq!(parse_with(&[("RUNID", "")]).unwrap().runid(), Some(""));
        assert_eq!(
            parse_with(&[("runid", "Run #1")]).unwrap().runid(),
            Some("Run #1")
        );
        let longest = "й".repeat(64);
        assert_eq!(
            parse_with(&[("RUNID", &longest)]).unwrap().runid(),
            Some(longest.as_str())
        );
        assert_invalid(&[("RUNID", &"й".repeat(65))], "RUNID");
    }

    #[test]
    fn upload_values() {
        let params = parse_with(&[
            ("UPLOAD", "d,HTTPS://example.com/d.xml"),
            ("upload", "b_2,param:b"),
            ("Upload", "C,vos://example.com!vospace/c.xml"),
            ("UPLOAD", "a,http://example.com/a.xml?x=1,2"),
            ("UPLOAD", "e,s3://bucket/e.parquet"),
        ])
        .unwrap();
        let uploads: Vec<_> = params
            .uploads()
            .map(|upload| (upload.table_name(), upload.source().clone()))
            .collect();
        assert_eq!(
            uploads,
            [
                (
                    "a",
                    UploadSource::Url(Url::parse("http://example.com/a.xml?x=1,2").unwrap())
                ),
                ("b_2", UploadSource::Inline("b".to_owned())),
                (
                    "C",
                    UploadSource::VoSpace("vos://example.com!vospace/c.xml".to_owned())
                ),
                (
                    "d",
                    UploadSource::Url(Url::parse("https://example.com/d.xml").unwrap())
                ),
                (
                    "e",
                    UploadSource::Url(Url::parse("s3://bucket/e.parquet").unwrap())
                ),
            ]
        );
    }

    /// TAP 1.0 form with several tables in one UPLOAD value.
    #[test]
    fn several_uploads_in_one_value() {
        let params = parse_with(&[
            ("UPLOAD", "a,param:a;b,http://example.com/b.xml"),
            ("UPLOAD", "c,param:c"),
        ])
        .unwrap();
        let names: Vec<_> = params.uploads().map(Upload::table_name).collect();
        assert_eq!(names, ["a", "b", "c"]);
        assert_eq!(
            parse_with(&[("UPLOAD", "a,param:a;b,param:a")]).unwrap(),
            parse_with(&[("UPLOAD", "a,param:a"), ("UPLOAD", "b,param:a")]).unwrap()
        );
    }

    #[test]
    fn upload_order_does_not_matter() {
        let a = ("UPLOAD", "a,param:a");
        let b = ("UPLOAD", "b,param:b");
        assert_eq!(parse_with(&[a, b]).unwrap(), parse_with(&[b, a]).unwrap());
    }

    #[test]
    fn invalid_uploads() {
        for upload in [
            "t",
            "t,",
            ",http://example.com/t.xml",
            "1t,http://example.com/t.xml",
            "_t,http://example.com/t.xml",
            "t-1,http://example.com/t.xml",
            "s.t,http://example.com/t.xml",
            "select,http://example.com/t.xml",
            "Size,param:t",
            "t,example.com/t.xml",
            "t,http://",
            "t,param:",
            "t,vos:",
            "t, http://example.com/t.xml",
            "t,http://example.com/t.xml ",
            "t ,http://example.com/t.xml",
            "a,param:a;",
            "a,param:a;;b,param:b",
        ] {
            assert_invalid(&[("UPLOAD", upload)], "UPLOAD");
        }
    }

    #[test]
    fn duplicate_upload_names() {
        let error = parse_with(&[("UPLOAD", "t,param:a"), ("UPLOAD", "T,param:b")]).unwrap_err();
        assert_eq!(error, TapQueryParamsError::DuplicateUpload("T".to_owned()));
    }
}
