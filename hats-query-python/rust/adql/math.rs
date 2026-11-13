//! Simple algebraic scalar functions
//!
//! List of mathematical function from ADQL 2.1 Section 2.1.4 "ADQL reserved keywords",
//! paragraph "Mathematical functions and operators":
//! ABS, ACOS, ASIN, ATAN, ATAN2,
//! CEILING, COS, COT, DEGREES, EXP, FLOOR, LOG, LOG10, MOD, PI, POWER,
//! RADIANS, RAND, ROUND, SIN, SQRT, TAN, TRUNCATE
//!
//! See Table 1 and 2 for mathematical function descriptions.
//!
//! DataFusion also implements a lot of mathematical functions, we declare them them with
//! "linccfw_" prefix as required by Section 4.3.1 of ADQL 2.1.

use super::ADQL_UDF_PREFIX;
use super::renamed::Renamed;
use datafusion::arrow::compute::kernels::numeric::rem;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DFResult;
use datafusion::functions::math;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion::prelude::*;
use std::sync::Arc;

/// DataFusion math functions that ADQL uses under the same name
const ADQL21_FUNCTIONS_SAME_NAME_AS_DATAFUSION: &[&str] = &[
    "abs", "acos", "asin", "atan", "atan2", "cos", "cot", "degrees", "exp", "floor", "log10", "pi",
    "power", "radians", "round", "sin", "sqrt", "tan",
];

/// Non-ADQL DataFusion math functions, exposed as UDFs with ADQL_UDF_PREFIX
fn datafusion_udfs() -> [Arc<ScalarUDF>; 16] {
    [
        math::acosh(),
        math::asinh(),
        math::atanh(),
        math::cbrt(),
        math::ceil(),
        math::cosh(),
        math::factorial(),
        math::gcd(),
        math::isnan(),
        math::iszero(),
        math::lcm(),
        math::log2(),
        math::nanvl(),
        math::signum(),
        math::sinh(),
        math::tanh(),
    ]
}

/// Registers ADQL mathematical functions in the session context.
pub(super) fn register_math_udfs(ctx: &SessionContext) {
    // Must go first, so we don't deregister functions with the same names registered below
    for udf in math::functions() {
        if !ADQL21_FUNCTIONS_SAME_NAME_AS_DATAFUSION.contains(&udf.name()) {
            ctx.deregister_udf(udf.name());
        }
    }

    ctx.register_udf(Renamed::new("ceiling", math::ceil()).into());
    ctx.register_udf(Renamed::new("log", math::ln()).into());
    ctx.register_udf(create_mod_function());
    ctx.register_udf(ScalarUDF::from(Rand::new()));
    ctx.register_udf(Renamed::new("truncate", math::round()).into());

    for udf in datafusion_udfs() {
        let name = format!("{ADQL_UDF_PREFIX}{}", udf.name());
        ctx.register_udf(Renamed::new(name, udf).into());
    }
}

/// ADQL MOD(x, y): remainder of x / y, defined for doubles only
fn create_mod_function() -> ScalarUDF {
    create_udf(
        "mod",
        vec![DataType::Float64, DataType::Float64],
        DataType::Float64,
        Volatility::Immutable,
        Arc::new(|args: &[ColumnarValue]| {
            let arrays = ColumnarValue::values_to_arrays(args)?;
            Ok(ColumnarValue::Array(rem(&arrays[0], &arrays[1])?))
        }),
    )
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Rand {
    signature: Signature,
}

impl Rand {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Nullary, TypeSignature::Any(1)],
                Volatility::Volatile,
            ),
        }
    }
}

impl ScalarUDFImpl for Rand {
    fn name(&self) -> &str {
        "rand"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Float64)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        math::random().invoke_with_args(ScalarFunctionArgs {
            args: vec![],
            arg_fields: vec![],
            ..args
        })
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::AsArray;
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::{DataType, Float64Type};
    use datafusion::common::Result as DFResult;
    use datafusion::execution::context::SessionContext;

    /// Runs a single-value query and returns the value as f64
    async fn query_f64(ctx: &SessionContext, query: &str) -> DFResult<f64> {
        let batches = ctx.sql(query).await?.collect().await?;
        let column = cast(batches[0].column(0), &DataType::Float64)?;
        Ok(column.as_primitive::<Float64Type>().value(0))
    }

    async fn check_function_no_args(ctx: &SessionContext, func: &str) -> DFResult<f64> {
        query_f64(ctx, &format!("SELECT {func}()")).await
    }

    async fn check_function_one_arg(ctx: &SessionContext, func: &str) -> DFResult<f64> {
        query_f64(ctx, &format!("SELECT {func}(2.0)")).await
    }

    async fn check_function_two_args(ctx: &SessionContext, func: &str) -> DFResult<f64> {
        // TRUNCATE's second argument is an integer, so we use 2 instead of 2.0
        query_f64(ctx, &format!("SELECT {func}(1.0, 2)")).await
    }

    #[tokio::test]
    async fn check_function_exists() {
        let ctx = SessionContext::new();
        super::register_math_udfs(&ctx);

        let functions_no_args = ["PI", "RAND"];
        for func in functions_no_args {
            assert!(
                check_function_no_args(&ctx, func).await.is_ok(),
                "Function {func} should exist"
            );
        }

        let functions_one_arg = [
            "ABS", "ACOS", "ASIN", "ATAN", "CEILING", "COS", "COT", "DEGREES", "EXP", "FLOOR",
            "LOG", "LOG10", "RADIANS", "RAND", "ROUND", "SIN", "SQRT", "TAN",
        ];
        for func in functions_one_arg {
            assert!(
                check_function_one_arg(&ctx, func).await.is_ok(),
                "Function {func} should exist"
            );
        }

        let functions_two_args = ["ATAN2", "MOD", "POWER", "TRUNCATE"];
        for func in functions_two_args {
            assert!(
                check_function_two_args(&ctx, func).await.is_ok(),
                "Function {func} should exist"
            );
        }
    }

    #[tokio::test]
    async fn check_log_is_natural_log() {
        let ctx = SessionContext::new();
        super::register_math_udfs(&ctx);

        let log_2_actual = check_function_one_arg(&ctx, "LOG").await.unwrap();
        let ln_2_expected = 2.0f64.ln();
        approx::assert_relative_eq!(log_2_actual, ln_2_expected);
    }
}
