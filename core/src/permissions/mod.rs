/// Types used for granting permissions within `PliantDb`.
pub mod pliant;

pub use actionable::{
    Action, ActionName, ActionNameList, Actionable, Dispatcher, Identifier, PermissionDenied,
    Permissions, ResourceName, Statement,
};
