//! Types protecting production by implementing limits on customer data.

use generated_types::influxdata::iox::namespace::{
    v1 as namespace_proto, v1::update_namespace_service_protection_limit_request::LimitUpdate,
};
use observability_deps::tracing::*;
use std::num::NonZeroUsize;
use thiserror::Error;

/// Definitions that apply to both MaxColumnsPerTable and MaxTables. Note that the hardcoded
/// default value specified in the macro invocation must be greater than 0 and fit in an `i32`.
macro_rules! define_service_limit {
    ($type_name:ident, $default_value:expr, $documentation:expr) => {
        /// $documentation
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $type_name(NonZeroUsize);

        impl TryFrom<usize> for $type_name {
            type Error = ServiceLimitError;

            fn try_from(value: usize) -> Result<Self, Self::Error> {
                // Even though the value is stored as a `usize`, service limits are stored as `i32`
                // in the database and transferred as i32 over protobuf. So try to convert to an
                // `i32` (and throw away the result) so that we know about invalid values before
                // trying to use them.
                if i32::try_from(value).is_err() {
                    return Err(ServiceLimitError::MustFitInI32);
                }

                let nonzero_value =
                    NonZeroUsize::new(value).ok_or(ServiceLimitError::MustBeGreaterThanZero)?;

                Ok(Self(nonzero_value))
            }
        }

        impl TryFrom<u64> for $type_name {
            type Error = ServiceLimitError;

            fn try_from(value: u64) -> Result<Self, Self::Error> {
                // Even though the value is stored as a `usize`, service limits are stored as `i32`
                // in the database and transferred as i32 over protobuf. So try to convert to an
                // `i32` (and throw away the result) so that we know about invalid values before
                // trying to use them.
                if i32::try_from(value).is_err() {
                    return Err(ServiceLimitError::MustFitInI32);
                }

                let nonzero_value = usize::try_from(value)
                    .ok()
                    .and_then(NonZeroUsize::new)
                    .ok_or(ServiceLimitError::MustBeGreaterThanZero)?;

                Ok(Self(nonzero_value))
            }
        }

        impl TryFrom<i32> for $type_name {
            type Error = ServiceLimitError;

            fn try_from(value: i32) -> Result<Self, Self::Error> {
                let nonzero_value = usize::try_from(value)
                    .ok()
                    .and_then(NonZeroUsize::new)
                    .ok_or(ServiceLimitError::MustBeGreaterThanZero)?;

                Ok(Self(nonzero_value))
            }
        }

        #[allow(missing_docs)]
        impl $type_name {
            pub fn get(&self) -> usize {
                self.0.get()
            }

            /// For use by the database and some protobuf representations. It should not be
            /// possible to construct an instance that contains a `NonZeroUsize` that won't fit in
            /// an `i32`.
            pub fn get_i32(&self) -> i32 {
                self.0.get() as i32
            }

            /// Constant-time default for use in constructing test constants.
            pub const fn const_default() -> Self {
                // This is safe because the hardcoded value is not 0.
                let value = unsafe { NonZeroUsize::new_unchecked($default_value) };

                Self(value)
            }
        }

        impl Default for $type_name {
            fn default() -> Self {
                Self::const_default()
            }
        }

        impl std::fmt::Display for $type_name {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        // Tell sqlx this is an i32 in the database.
        impl<DB> sqlx::Type<DB> for $type_name
        where
            i32: sqlx::Type<DB>,
            DB: sqlx::Database,
        {
            fn type_info() -> DB::TypeInfo {
                <i32 as sqlx::Type<DB>>::type_info()
            }
        }

        impl<'q, DB> sqlx::Encode<'q, DB> for $type_name
        where
            DB: sqlx::Database,
            i32: sqlx::Encode<'q, DB>,
        {
            fn encode_by_ref(
                &self,
                buf: &mut <DB as sqlx::database::HasArguments<'q>>::ArgumentBuffer,
            ) -> sqlx::encode::IsNull {
                <i32 as sqlx::Encode<'_, DB>>::encode_by_ref(&self.get_i32(), buf)
            }
        }

        // The database stores i32s, so there's a chance of invalid values already being stored in
        // there. When deserializing those values, rather than panicking or returning an error, log
        // and use the default instead.
        impl<'r, DB: ::sqlx::Database> ::sqlx::decode::Decode<'r, DB> for $type_name
        where
            i32: sqlx::Decode<'r, DB>,
        {
            fn decode(
                value: <DB as ::sqlx::database::HasValueRef<'r>>::ValueRef,
            ) -> ::std::result::Result<
                Self,
                ::std::boxed::Box<
                    dyn ::std::error::Error + 'static + ::std::marker::Send + ::std::marker::Sync,
                >,
            > {
                let data = <i32 as ::sqlx::decode::Decode<'r, DB>>::decode(value)?;

                let data = Self::try_from(data).unwrap_or_else(|_| {
                    error!("database contains invalid $type_name value {data}");
                    Self::default()
                });

                Ok(data)
            }
        }
    };
}

define_service_limit!(MaxTables, 500, "Max tables allowed in a namespace.");
define_service_limit!(
    MaxColumnsPerTable,
    200,
    "Max columns per table allowed in a namespace."
);

/// Overrides for service protection limits.
#[derive(Debug, Copy, Clone)]
pub struct NamespaceServiceProtectionLimitsOverride {
    /// The maximum number of tables that can exist in this namespace
    pub max_tables: Option<MaxTables>,
    /// The maximum number of columns per table in this namespace
    pub max_columns_per_table: Option<MaxColumnsPerTable>,
}

impl TryFrom<namespace_proto::ServiceProtectionLimits>
    for NamespaceServiceProtectionLimitsOverride
{
    type Error = ServiceLimitError;

    fn try_from(value: namespace_proto::ServiceProtectionLimits) -> Result<Self, Self::Error> {
        let namespace_proto::ServiceProtectionLimits {
            max_tables,
            max_columns_per_table,
        } = value;

        Ok(Self {
            max_tables: max_tables.map(MaxTables::try_from).transpose()?,
            max_columns_per_table: max_columns_per_table
                .map(MaxColumnsPerTable::try_from)
                .transpose()?,
        })
    }
}

/// Updating one, but not both, of the limits is what the UpdateNamespaceServiceProtectionLimit
/// gRPC request supports, so match that encoding on the Rust side.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceLimitUpdate {
    /// Requesting an update to the maximum number of tables allowed in this namespace
    MaxTables(MaxTables),
    /// Requesting an update to the maximum number of columns allowed in each table in this
    /// namespace
    MaxColumnsPerTable(MaxColumnsPerTable),
}

/// Errors converting from raw values to the service limits
#[derive(Error, Debug, Clone, Copy)]
pub enum ServiceLimitError {
    /// A negative or 0 value was specified; those aren't allowed
    #[error("service limit values must be greater than 0")]
    MustBeGreaterThanZero,

    /// No value was provided so we can't update anything
    #[error("a supported service limit value is required")]
    NoValueSpecified,

    /// Limits are stored as `i32` in the database and transferred as i32 over protobuf, so even
    /// though they are stored as `usize` in Rust, the `usize` value must be less than `i32::MAX`.
    #[error("service limit values must fit in a 32-bit signed integer (`i32`)")]
    MustFitInI32,
}

impl TryFrom<Option<LimitUpdate>> for ServiceLimitUpdate {
    type Error = ServiceLimitError;

    fn try_from(limit_update: Option<LimitUpdate>) -> Result<Self, Self::Error> {
        match limit_update {
            Some(LimitUpdate::MaxTables(n)) => {
                Ok(ServiceLimitUpdate::MaxTables(MaxTables::try_from(n)?))
            }
            Some(LimitUpdate::MaxColumnsPerTable(n)) => Ok(ServiceLimitUpdate::MaxColumnsPerTable(
                MaxColumnsPerTable::try_from(n)?,
            )),
            None => Err(ServiceLimitError::NoValueSpecified),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::i32;

    #[test]
    fn max_tables_from_usize() {
        assert_eq!(MaxTables::try_from(1usize).unwrap().get(), 1);

        assert_eq!(
            MaxTables::try_from(0usize).unwrap_err().to_string(),
            "service limit values must be greater than 0"
        );
        assert_eq!(
            MaxTables::try_from(i32::MAX as usize + 1)
                .unwrap_err()
                .to_string(),
            "service limit values must fit in a 32-bit signed integer (`i32`)"
        );
    }

    #[test]
    fn max_tables_from_u64() {
        assert_eq!(MaxTables::try_from(1u64).unwrap().get(), 1);

        assert_eq!(
            MaxTables::try_from(0u64).unwrap_err().to_string(),
            "service limit values must be greater than 0"
        );
        assert_eq!(
            MaxTables::try_from(i32::MAX as u64 + 1)
                .unwrap_err()
                .to_string(),
            "service limit values must fit in a 32-bit signed integer (`i32`)"
        );
    }

    #[test]
    fn max_tables_from_i32() {
        assert_eq!(MaxTables::try_from(1i32).unwrap().get(), 1);
        assert_eq!(
            MaxTables::try_from(i32::MAX).unwrap().get(),
            i32::MAX as usize
        );

        assert_eq!(
            MaxTables::try_from(0i32).unwrap_err().to_string(),
            "service limit values must be greater than 0"
        );
        assert_eq!(
            MaxTables::try_from(-1i32).unwrap_err().to_string(),
            "service limit values must be greater than 0"
        );
    }
}
