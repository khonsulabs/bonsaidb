# Users, Groups, and Roles

The most common flow that a database administrator needs to support is granting a user the ability to take specific actions on specific resources. To accomplish this, a [`PermissionGroup`](https://bonsaidb.dev/main/bonsaidb/core/admin/struct.PermissionGroup.html) must be created containing the permission statements, covered in [the previous section](./permission-statements.md), that you wish to apply.

`PermissionGroup`s can be assigned directly to users by adding the group ID to their [`User` document](https://bonsaidb.dev/main/bonsaidb/core/admin/struct.User.html).

At first glance, [`Role`s](https://bonsaidb.dev/main/bonsaidb/core/admin/struct.Role.html) may appear somewhat redundant. One or more `PermissionGroup`s can be assigned to a role, and roles can be assigned to a user. Why would you want to use roles at all?

The general advice the authors of `BonsaiDb` suggest is to use groups for limited amounts of functionality, keeping each group's list of statements concise and easy to understand. Then, create roles that combine groups of functionality in meaningful ways. One meaningful way could be creating roles based on job titles inside of a company. In theory, a person's job defines what they do within the company.

In practice, permissions are never as clean as one would hope, which is why `BonsaiDb` allows assigning groups and roles to users directly. Roles should be used as much as possible, but sometimes assigning a group directly is just needed. For example, imagine the CEO telling you, "I know Bob is just a sales guy, but he needs to be able to update this record. I trust him more than the other sales people. Just make it happen." As the database administrator, you can decide whether to introduce a new role or just temporarily assign an extra group to this one user.
