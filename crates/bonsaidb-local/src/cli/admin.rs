use bonsaidb_core::connection::{AsyncStorageConnection, StorageConnection};
use clap::Subcommand;

/// An administrative command-line command.
#[derive(Subcommand, Debug)]
pub enum Command {
    /// A command operating on [`User`s](bonsaidb_core::admin::User).
    #[clap(subcommand)]
    User(UserCommand),
}

/// A command operating on [`User`s](bonsaidb_core::admin::User).
#[derive(Subcommand, Debug)]
pub enum UserCommand {
    /// Creates a new user.
    Create {
        /// The username of the user to create.
        username: String,
        /// If this flag is provided, the user's initial password will be
        /// prompted for over stdin before creating the user.
        #[cfg(feature = "password-hashing")]
        #[clap(long)]
        password: bool,
    },
    /// Sets an existing user's password. The password will be prompted for over
    /// stdin.
    #[cfg(feature = "password-hashing")]
    SetPassword {
        /// The username of the user to change the password of.
        username: String,
    },
    /// Adds a role to a user.
    AddRole {
        /// The username to add the role to.
        username: String,
        /// The name of the role to add.
        role: String,
    },
    /// Removes a role from user.
    RemoveRole {
        /// The username to remove the role from.
        username: String,
        /// The name of the role to remove.
        role: String,
    },
    /// Adds a role to a user.
    AddGroup {
        /// The username to add the role to.
        username: String,
        /// The name of the role to add.
        group: String,
    },
    /// Removes a permission group from user.
    RemoveGroup {
        /// The username to remove the permission group from.
        username: String,
        /// The name of the permission group to remove.
        group: String,
    },
}

impl Command {
    /// Executes the command on `storage`.
    pub fn execute<SC: StorageConnection>(self, storage: &SC) -> Result<(), crate::Error> {
        match self {
            Command::User(user) => match user {
                UserCommand::Create { username, password } => {
                    #[cfg(feature = "password-hashing")]
                    let password = if password {
                        Some(super::read_password_from_stdin(true)?)
                    } else {
                        None
                    };
                    let user_id = storage.create_user(&username)?;

                    #[cfg(feature = "password-hashing")]
                    if let Some(password) = password {
                        storage.set_user_password(user_id, password)?;
                    }

                    println!("User #{user_id} {username} created");
                    Ok(())
                }
                #[cfg(feature = "password-hashing")]
                UserCommand::SetPassword { username } => {
                    let password = super::read_password_from_stdin(true)?;
                    storage.set_user_password(&username, password)?;
                    println!("User {username}'s password has been updated.");
                    Ok(())
                }
                UserCommand::AddRole { username, role } => {
                    storage.add_role_to_user(&username, &role)?;
                    println!("Role {role} added to {username}");
                    Ok(())
                }
                UserCommand::RemoveRole { username, role } => {
                    storage.remove_role_from_user(&username, &role)?;
                    println!("Role {role} removed from {username}");
                    Ok(())
                }
                UserCommand::AddGroup { username, group } => {
                    storage.add_permission_group_to_user(&username, &group)?;
                    println!("Group {group} added to {username}");
                    Ok(())
                }
                UserCommand::RemoveGroup { username, group } => {
                    storage.remove_permission_group_from_user(&username, &group)?;
                    println!("Group {group} removed from {username}");
                    Ok(())
                }
            },
        }
    }

    /// Executes the command on `storage`.
    pub async fn execute_async<SC: AsyncStorageConnection>(
        self,
        storage: &SC,
    ) -> Result<(), crate::Error> {
        match self {
            Command::User(user) => match user {
                UserCommand::Create { username, password } => {
                    #[cfg(feature = "password-hashing")]
                    let password = if password {
                        Some(super::read_password_from_stdin(true)?)
                    } else {
                        None
                    };
                    let user_id = storage.create_user(&username).await?;

                    #[cfg(feature = "password-hashing")]
                    if let Some(password) = password {
                        storage.set_user_password(user_id, password).await?;
                    }

                    println!("User #{user_id} {username} created");
                    Ok(())
                }
                #[cfg(feature = "password-hashing")]
                UserCommand::SetPassword { username } => {
                    let password = super::read_password_from_stdin(true)?;
                    storage.set_user_password(&username, password).await?;
                    println!("User {username}'s password has been updated.");
                    Ok(())
                }
                UserCommand::AddRole { username, role } => {
                    storage.add_role_to_user(&username, &role).await?;
                    println!("Role {role} added to {username}");
                    Ok(())
                }
                UserCommand::RemoveRole { username, role } => {
                    storage.remove_role_from_user(&username, &role).await?;
                    println!("Role {role} removed from {username}");
                    Ok(())
                }
                UserCommand::AddGroup { username, group } => {
                    storage
                        .add_permission_group_to_user(&username, &group)
                        .await?;
                    println!("Group {group} added to {username}");
                    Ok(())
                }
                UserCommand::RemoveGroup { username, group } => {
                    storage
                        .remove_permission_group_from_user(&username, &group)
                        .await?;
                    println!("Group {group} removed from {username}");
                    Ok(())
                }
            },
        }
    }
}
