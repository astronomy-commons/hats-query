//! Tools for ADQL support.

mod common;
mod identifier;
mod math;
mod parser;
mod register;
mod renamed;
mod top;

use common::ADQL_UDF_PREFIX;
pub(crate) use identifier::delimit_if_needed;
pub(crate) use parser::AdqlContext;
pub(crate) use register::register_adql;
