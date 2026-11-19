//! Common code of the hats-query server and its Python bindings.

/// Greeting of the hats-query server, a placeholder until the code is moved here.
pub fn greeting() -> &'static str {
    "Hello from hats-query"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn greeting_mentions_hats_query() {
        assert!(greeting().contains("hats-query"));
    }
}
