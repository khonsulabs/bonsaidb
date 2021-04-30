use std::{borrow::Cow, collections::HashMap};

pub use pliantdb_macros::Action;

/// A single element of a [`ResourceName`]
#[derive(Debug, Hash, Eq, PartialEq, Clone)]
pub enum Identifier<'a> {
    /// When checking for allowed permissions, allow any match where this identifier is used.
    Any,
    /// An integer identifier.
    Integer(u64),
    /// A string identifier.
    String(Cow<'a, str>),
}

impl<'a> From<u64> for Identifier<'a> {
    fn from(id: u64) -> Self {
        Self::Integer(id)
    }
}

impl<'a> From<&'a str> for Identifier<'a> {
    fn from(id: &'a str) -> Self {
        Self::String(Cow::Borrowed(id))
    }
}

impl<'a> From<String> for Identifier<'a> {
    fn from(id: String) -> Self {
        Self::String(Cow::Owned(id))
    }
}

/// An action that can be allowed or disalled.
pub trait Action {
    /// The full name of this action.
    fn name(&self) -> ActionName;
}

impl Action for () {
    fn name(&self) -> ActionName {
        ActionName::default()
    }
}

/// A collection of allowed permissions.
#[derive(Default, Debug)]
pub struct Permissions {
    children: Option<HashMap<Identifier<'static>, Permissions>>,
    allowed: AllowedActions,
}

#[derive(Debug)]
enum AllowedActions {
    None,
    Some(HashMap<String, AllowedActions>),
    All,
}

impl Default for AllowedActions {
    fn default() -> Self {
        Self::None
    }
}

impl Permissions {
    /// Evaluate whether the `action` is allowed to be taken upon
    /// `resource_name`. Returns true if the action should be allowed. If no
    /// statements that match `resource_name` allow `action`, false will be
    /// returned.
    pub fn allowed_to<R: AsRef<[Identifier<'static>]>, P: Action>(
        &self,
        resource_name: R,
        action: &P,
    ) -> bool {
        let resource_name = resource_name.as_ref();
        // This function checks all possible matches of `resource_name` by using
        // recursion to call itself for each entry in `resource_name`. This
        // first block does the function call recursion. The second block checks
        // `action`.
        if let Some(resource) = resource_name.first() {
            if let Some(children) = &self.children {
                let remaining_resource = &resource_name[1..resource_name.len()];
                // Check if there are entries for this resource segment.
                if let Some(permissions) = children.get(resource) {
                    println!("Checking allowed in {:?}", resource);
                    if permissions.allowed_to(remaining_resource, action) {
                        return dbg!(true);
                    }
                    println!("Nope");
                }

                // Check if there are entries for `Any`.
                if let Some(permissions) = children.get(&Identifier::Any) {
                    println!("Checking allowed in Any");
                    if permissions.allowed_to(remaining_resource, action) {
                        return dbg!(true);
                    }
                    println!("Nope");
                }
            }
        }

        // When execution reaches here, either resource_name is empty, or none
        // of the previous paths have reached an "allow" state. The purpose of
        // this chunk of code is to determine if this action is allowed based on
        // this node's list of approved actions. This is also evaluated
        // recursively, but at any stage if we reach match (positive or
        // negative), we we can return.
        let mut allowed = &self.allowed;
        for name in action.name() {
            println!("checking permissions for {:?} in {:?}", name, allowed);
            allowed = match allowed {
                AllowedActions::None => return dbg!(false),
                AllowedActions::All => return dbg!(true),
                AllowedActions::Some(actions) => {
                    if let Some(children_allowed) = actions.get(name) {
                        children_allowed
                    } else {
                        println!("Didn't find child: {:?}", name);
                        return false;
                    }
                }
            };
        }
        dbg!(matches!(allowed, AllowedActions::All))
    }
}

impl From<Vec<Statement>> for Permissions {
    fn from(statements: Vec<Statement>) -> Self {
        let mut permissions = Self::default();
        for statement in statements {
            // Apply this statement to all resources
            for resource in statement.resources {
                let mut current_permissions = &mut permissions;
                // Look up the permissions for the resource path
                for name in resource {
                    let permissions = current_permissions
                        .children
                        .get_or_insert_with(HashMap::default);
                    current_permissions = permissions.entry(name).or_default();
                }

                // Apply the "allowed" status to each action in this resource.
                let mut allowed = &mut current_permissions.allowed;
                match &statement.actions {
                    ActionNameList::List(actions) => {
                        for action in actions {
                            for &name in &action.0 {
                                let action_map = match allowed {
                                    AllowedActions::All | AllowedActions::None => {
                                        *allowed = {
                                            let mut action_map = HashMap::new();
                                            action_map
                                                .insert(name.to_string(), AllowedActions::None);
                                            AllowedActions::Some(action_map)
                                        };
                                        if let AllowedActions::Some(action_map) = allowed {
                                            action_map
                                        } else {
                                            unreachable!()
                                        }
                                    }
                                    AllowedActions::Some(action_map) => action_map,
                                };
                                allowed = action_map.entry(name.to_string()).or_default();
                            }
                        }
                    }
                    ActionNameList::All => {}
                }

                if statement.allowed {
                    *allowed = AllowedActions::All
                } else {
                    *allowed = AllowedActions::None
                }
            }
        }
        permissions
    }
}

/// A statement of permissions. A statement describes whether one or more
/// `actions` should be `allowed` to be taken against `resources`.
#[derive(Debug)]
pub struct Statement {
    /// The list of resources this statement applies to.
    pub resources: Vec<ResourceName>,
    /// The list of actions this statement applies to.
    pub actions: ActionNameList,
    /// Whether the `actions` should be allowed or disallowed.
    pub allowed: bool,
}

/// A list of [`ActionName`]s.
#[derive(Debug)]
pub enum ActionNameList {
    /// A specific list of names.
    List(Vec<ActionName>),
    /// All actions.
    All,
}

impl<T> From<T> for ActionNameList
where
    T: Action,
{
    fn from(action: T) -> Self {
        Self::List(vec![action.name()])
    }
}

impl From<ActionName> for ActionNameList {
    fn from(name: ActionName) -> Self {
        Self::List(vec![name])
    }
}

impl From<Vec<ActionName>> for ActionNameList {
    fn from(names: Vec<ActionName>) -> Self {
        Self::List(names)
    }
}

/// A unique name/identifier of a resource.
#[derive(Default, Debug)]
pub struct ResourceName(Vec<Identifier<'static>>);

impl ResourceName {
    /// Creates a `ResourceName` that matches any identifier.
    #[must_use]
    pub fn any() -> Self {
        Self::named(Identifier::Any)
    }

    /// Creates a `ResourceName` with `name`.
    #[must_use]
    pub fn named<I: Into<Identifier<'static>>>(name: I) -> Self {
        Self(vec![name.into()])
    }

    /// Adds another name segment.
    #[must_use]
    pub fn and<I: Into<Identifier<'static>>>(mut self, name: I) -> Self {
        self.0.push(name.into());
        self
    }
}

impl AsRef<[Identifier<'static>]> for ResourceName {
    fn as_ref(&self) -> &[Identifier<'static>] {
        &self.0
    }
}

impl IntoIterator for ResourceName {
    type Item = Identifier<'static>;

    type IntoIter = std::vec::IntoIter<Identifier<'static>>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// A unique name of an action.
#[derive(Default, Debug)]
pub struct ActionName(pub Vec<&'static str>);

impl From<Vec<&'static str>> for ActionName {
    fn from(inner: Vec<&'static str>) -> Self {
        Self(inner)
    }
}

impl IntoIterator for ActionName {
    type Item = &'static str;

    type IntoIter = std::vec::IntoIter<&'static str>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Action)]
    enum TestActions {
        DoSomething,
        Post(PostActions),
    }

    #[derive(Debug, Action)]
    enum PostActions {
        Read,
        Update,
        Delete,
    }

    #[test]
    fn basics() {
        // Default action is deny
        let statements = vec![
            // Allow Read on all.
            Statement {
                resources: vec![ResourceName::any()],
                actions: ActionNameList::from(TestActions::Post(PostActions::Read)),
                allowed: true,
            },
            // Allow all actions for the resource named all-actions-allowed
            Statement {
                resources: vec![ResourceName::named("all-actions-allowed")],
                actions: ActionNameList::All,
                allowed: true,
            },
            // Allow all Post actions for the resource named only-post-actions-allowed
            Statement {
                resources: vec![ResourceName::named("only-post-actions-allowed")],
                actions: ActionNameList::from(ActionName::from(vec!["Post"])),
                allowed: true,
            },
        ];
        let permissions = Permissions::from(statements);

        // Check the positive cases:
        assert!(permissions.allowed_to(
            &ResourceName::named("someresource"),
            &TestActions::Post(PostActions::Read)
        ));
        assert!(permissions.allowed_to(
            &ResourceName::named("all-actions-allowed"),
            &TestActions::Post(PostActions::Update)
        ));
        assert!(permissions.allowed_to(
            &ResourceName::named("all-actions-allowed"),
            &TestActions::DoSomething
        ));
        assert!(permissions.allowed_to(
            &ResourceName::named("only-post-actions-allowed"),
            &TestActions::Post(PostActions::Delete)
        ));

        // Test the negatives
        assert!(!permissions.allowed_to(
            &ResourceName::named("someresource"),
            &TestActions::Post(PostActions::Update)
        ));
        assert!(!permissions.allowed_to(
            &ResourceName::named("someresource"),
            &TestActions::Post(PostActions::Delete)
        ));
        assert!(!permissions.allowed_to(
            &ResourceName::named("only-post-actions-allowed"),
            &TestActions::DoSomething
        ));
    }

    #[test]
    fn precedence_test() {
        // Allo
        let statements = vec![
            Statement {
                resources: vec![ResourceName::any().and("users").and(1)],
                actions: ActionNameList::from(TestActions::Post(PostActions::Read)),
                allowed: true,
            },
            Statement {
                resources: vec![ResourceName::named("some-database").and("users").and(1)],
                actions: ActionNameList::from(TestActions::Post(PostActions::Update)),
                allowed: true,
            },
            Statement {
                resources: vec![ResourceName::named("some-database")
                    .and("users")
                    .and(Identifier::Any)],
                actions: ActionNameList::from(TestActions::Post(PostActions::Delete)),
                allowed: true,
            },
            Statement {
                resources: vec![ResourceName::named("some-database")
                    .and(Identifier::Any)
                    .and(1)],
                actions: ActionNameList::from(TestActions::DoSomething),
                allowed: true,
            },
        ];
        let permissions = Permissions::from(statements);
    }
}
