//! Connection strings that do not leak their password.
//!
//! `Settings` derives `Debug` and gets logged at startup, so a plain `String`
//! DSN would put `postgres://user:hunter2@host/db` straight into the logs.
//! [`Dsn`] keeps the raw value reachable only through [`Dsn::expose`]; every
//! formatting path redacts the userinfo password instead.

use std::fmt;

use serde::Deserialize;

const REDACTED: &str = "***";

/// A connection string whose `Debug` and `Display` output is redacted.
#[derive(Clone, Deserialize, PartialEq, Eq, Hash)]
#[serde(transparent)]
pub struct Dsn(String);

impl Dsn {
    pub fn new(raw: impl Into<String>) -> Self {
        Self(raw.into())
    }

    /// The raw connection string. Pass it to a driver, never to a log.
    pub fn expose(&self) -> &str {
        &self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.trim().is_empty()
    }

    /// The connection string with the password replaced by `***`.
    pub fn redacted(&self) -> String {
        let Some((scheme, rest)) = self.0.split_once("://") else {
            // No scheme means we cannot locate the userinfo, so assume the
            // worst and redact the lot.
            return REDACTED.to_owned();
        };

        // The authority ends at the first `/`, `?` or `#`; a password may
        // itself contain `@`, so take the *last* `@` inside the authority.
        let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
        let (authority, tail) = rest.split_at(authority_end);

        let Some((userinfo, host)) = authority.rsplit_once('@') else {
            return self.0.clone();
        };
        let user = userinfo.split_once(':').map_or(userinfo, |(user, _)| user);

        format!("{scheme}://{user}:{REDACTED}@{host}{tail}")
    }
}

impl fmt::Debug for Dsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.redacted())
    }
}

impl fmt::Display for Dsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.redacted())
    }
}

impl From<&str> for Dsn {
    fn from(raw: &str) -> Self {
        Self::new(raw)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn password_is_redacted() {
        let dsn = Dsn::new("postgres://orderbook:hunter2@db.internal:5432/orderbook");

        assert_eq!(
            dsn.redacted(),
            "postgres://orderbook:***@db.internal:5432/orderbook"
        );
        assert_eq!(format!("{dsn}"), dsn.redacted());
        assert_eq!(format!("{dsn:?}"), dsn.redacted());
    }

    #[test]
    fn password_containing_an_at_sign_is_still_redacted() {
        let dsn = Dsn::new("redis://default:p@ss@w0rd@cache:6379/0");

        assert_eq!(dsn.redacted(), "redis://default:***@cache:6379/0");
    }

    #[test]
    fn query_string_is_preserved() {
        let dsn = Dsn::new("postgres://u:p@host/db?sslmode=require&application_name=obe");

        assert_eq!(
            dsn.redacted(),
            "postgres://u:***@host/db?sslmode=require&application_name=obe"
        );
    }

    #[test]
    fn userinfo_without_a_password_gains_one() {
        // The shape stays uniform, so a reader cannot tell from the log
        // whether a password was configured.
        let dsn = Dsn::new("postgres://orderbook@localhost/orderbook");

        assert_eq!(
            dsn.redacted(),
            "postgres://orderbook:***@localhost/orderbook"
        );
    }

    #[test]
    fn dsn_without_userinfo_is_unchanged() {
        let dsn = Dsn::new("redis://localhost:6379");

        assert_eq!(dsn.redacted(), "redis://localhost:6379");
    }

    #[test]
    fn unparseable_dsn_is_fully_redacted() {
        assert_eq!(Dsn::new("user:pass@host/db").redacted(), "***");
    }

    #[test]
    fn expose_returns_the_raw_value() {
        let raw = "postgres://u:p@host/db";

        assert_eq!(Dsn::new(raw).expose(), raw);
    }
}
