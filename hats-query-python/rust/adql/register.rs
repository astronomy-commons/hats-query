use crate::adql::math::register_math_udfs;
use datafusion::prelude::*;

pub(crate) fn register_adql(ctx: &SessionContext) {
    register_math_udfs(ctx);
}
