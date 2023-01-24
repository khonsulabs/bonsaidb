use actionable::{Action, Identifier};

use crate::connection::Session;
use crate::Error;

/// Functions to access information about the current session (authentication).
pub trait HasSession {
    /// Returns the currently authenticated session, if any.
    fn session(&self) -> Option<&Session>;

    /// Checks if `action` is permitted against `resource_name`.
    fn allowed_to<'a, R: AsRef<[Identifier<'a>]>, P: Action>(
        &self,
        resource_name: R,
        action: &P,
    ) -> bool {
        self.session()
            .map_or(true, |session| session.allowed_to(resource_name, action))
    }

    /// Checks if `action` is permitted against `resource_name`. If permission
    /// is denied, returns a [`PermissionDenied`](Error::PermissionDenied)
    /// error.
    fn check_permission<'a, R: AsRef<[Identifier<'a>]>, P: Action>(
        &self,
        resource_name: R,
        action: &P,
    ) -> Result<(), Error> {
        self.session().map_or_else(
            || Ok(()),
            |session| session.check_permission(resource_name, action),
        )
    }
}
