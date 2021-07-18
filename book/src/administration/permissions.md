# Permissions

`BonsaiDb` uses [role-based access control (RBAC)](https://en.wikipedia.org/wiki/Role-based_access_control). In short, permissions are granted through statements within permission groups. Users are able to [log in](https://bonsaidb.dev/main/bonsaidb/client/struct.Client.html#method.login_with_password_str) and receive permissions that were granted via permission groups or roles.

This section has two subsections:

- [Permission Statements](./permission-statements.md): An overview of the resource names and actions used within `BonsaiDb`.
- [Users, Groups, and Roles](./rbac.md): A more thorough explanation of `BonsaiDb`'s access control.

While the most common use case will be granting permissions to act upon `BonsaiDb` itself, the permissions system is designed to be generic enough that it can be used as the application's permission system if desired.

By default, no actions are allowed.

Currently, permissions are only applied to connections over a network. In [the future](https://github.com/khonsulabs/bonsaidb/issues/68), permissions will be able to be applied even on local connections.
