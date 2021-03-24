# PliantDB

[![Live Build Status](https://img.shields.io/github/workflow/status/khonsulabs/pliantdb/Tests/main)](https://github.com/khonsulabs/pliantdb/actions?query=workflow:Tests)
[![codecov](https://codecov.io/gh/khonsulabs/pliantdb/branch/main/graph/badge.svg)](https://codecov.io/gh/khonsulabs/pliantdb)
[![Documentation for `main` branch](https://img.shields.io/badge/docs-main-informational)](https://khonsulabs.github.io/pliantdb/main/pliantdb/)
[![HTML Coverage Report for `main` branch](https://img.shields.io/badge/coverage-report-informational)](https://khonsulabs.github.io/pliantdb/coverage/)

PliantDB aims to be a [Rust](https://rust-lang.org)-written document database inspired by [CouchDB](https://couchdb.apache.org/). While it is inspired by CouchDB, this project will not aim to be compatible with existing CouchDB servers, and it will be implementing its own replication, clustering, and sharding strategies.

The high-level goals for this project are:

- Be able to a document-based database's schema using Rust types.
- Run within your Rust binary, simplifying basic deployments.
- Run stand-alone, allowing more complex deployments and scaling your app separately from scaling the database.
- Run in a single-database, local-only mode with no networking involved.
- Run in a multi-database and networked mode with TLS enabled by default.
- Easily set up read-replicas between multiple servers.
- Easily run a highly-available quorum-based cluster across at least 3 servers
- Expose a Publish/Subscribe eventing system
- Expose a Job queue and scheduling system -- a la [Sidekiq](https://sidekiq.org/) or [SQS](https://aws.amazon.com/sqs/)

## Why write another database?

- Deploying highly-available databases is hard (and often expensive). It doesn't need to be.
- We are passionate Rustaceans and are striving for an ideal of supporting a 100% Rust-based deployment ecosystem for newly written software.
- Specifically for the founding author [@ecton](https://github.com/ecton), the idea for this design dates back to thoughts of fun side-projects while running my last business which was built atop CouchDB. Working on this project is fulfilling a long-time desire of his.

## Status of this project

This project is hopefully still under active development (![GitHub commit activity](https://img.shields.io/github/commit-activity/m/khonsulabs/pliantdb)), but at the point of writing this README, the project is too early to be used. Unless this README has been updated otherwise, you should not attempt to use this software in anything except for experiments.

If you're interested in chatting about this project or potentially wanting to contribute, come chat with us on Discord: [![Discord](https://img.shields.io/discord/578968877866811403)](https://discord.khonsulabs.com/).

## Open-source Licenses

This project, like all projects from [Khonsu Labs](https://khonsulabs.com/), are open-source. This repository is available under the [MIT License](./LICENSE-MIT) or the [Apache License 2.0](./LICENSE-APACHE).
