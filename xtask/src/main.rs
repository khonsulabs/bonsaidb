use std::env::{current_dir, set_current_dir, temp_dir};
use std::io::{stdout, Write};
use std::str::FromStr;

use khonsu_tools::publish;
use khonsu_tools::universal::clap::{self, Parser};
use khonsu_tools::universal::devx_cmd::{run, Cmd};
use khonsu_tools::universal::{anyhow, audit, DefaultConfig};
use serde::Serialize;

#[derive(Parser, Debug)]
pub enum Commands {
    TestMatrix,
    Test {
        #[clap(long)]
        fail_on_warnings: bool,
    },
    #[clap(flatten)]
    Tools(khonsu_tools::Commands),
}

fn main() -> anyhow::Result<()> {
    if std::env::args().len() > 1 {
        let command = Commands::parse();
        match command {
            Commands::TestMatrix => generate_test_matrix_output(),
            Commands::Test { fail_on_warnings } => run_all_tests(fail_on_warnings),
            Commands::Tools(command) => command.execute::<Config>(),
        }
    } else {
        run_all_tests(true)
    }
}

enum Config {}

impl khonsu_tools::Config for Config {
    type Publish = Self;
    type Universal = Self;
}

impl khonsu_tools::universal::Config for Config {
    type Audit = Self;
    type CodeCoverage = DefaultConfig;
}

impl audit::Config for Config {
    fn args() -> Vec<String> {
        vec![
            String::from("--all-features"),
            String::from("--exclude=xtask"),
            String::from("--exclude=benchmarks"),
            // examples that include other dependencies, which aren't actually
            // indicative of the security of BonsaiDb.
            String::from("--exclude=axum"),
            String::from("--exclude=acme"),
            String::from("--exclude=view-histogram"),
        ]
    }
}

impl publish::Config for Config {
    fn paths() -> Vec<String> {
        vec![
            String::from("crates/bonsaidb-macros"),
            String::from("crates/bonsaidb-core"),
            String::from("crates/bonsaidb-utils"),
            String::from("crates/bonsaidb-local"),
            String::from("crates/bonsaidb-server"),
            String::from("crates/bonsaidb-client"),
            String::from("crates/bonsaidb-keystorage-s3"),
            String::from("crates/bonsaidb"),
        ]
    }
}

#[derive(Serialize)]
struct TestSuite {
    cargo_args: &'static str,
    toolchain: &'static str,
}

#[derive(Serialize)]
struct TestMatrix {
    include: &'static [TestSuite],
}

fn all_tests() -> &'static [TestSuite] {
    &[
        TestSuite {
            cargo_args: "--all-features",
            toolchain: "stable",
        },
        TestSuite {
            cargo_args: "--all-features",
            toolchain: "1.64",
        },
        TestSuite {
            cargo_args: "--package bonsaidb-core --no-default-features",
            toolchain: "stable",
        },
        TestSuite {
            cargo_args: "--package bonsaidb-local --no-default-features",
            toolchain: "stable",
        },
        TestSuite {
            cargo_args: "--package bonsaidb-local --no-default-features --features encryption",
            toolchain: "stable",
        },
        TestSuite {
            cargo_args: "--package bonsaidb-local --no-default-features --features compression",
            toolchain: "stable",
        },
        TestSuite {
            cargo_args: "--package bonsaidb-local --no-default-features --features async",
            toolchain: "stable",
        },
        TestSuite {
            cargo_args: "--package bonsaidb-local --no-default-features --features password-hashing",
            toolchain: "stable",
        },
        TestSuite {
            cargo_args: "--package bonsaidb-local --no-default-features --features token-authentication",
            toolchain: "stable",
        },
        TestSuite {
            cargo_args: "--package bonsaidb-local --no-default-features --features encryption,compression",
            toolchain: "stable",
        },
        TestSuite {
            cargo_args: "--package bonsaidb-server --no-default-features",
            toolchain: "stable",
        },
        TestSuite {
            cargo_args: "--package bonsaidb-server --no-default-features --features password-hashing",
            toolchain: "stable",
        },
        TestSuite {
            cargo_args: "--package bonsaidb-server --no-default-features --features token-authentication",
            toolchain: "stable",
        },
        TestSuite {
            cargo_args: "--package bonsaidb-server --no-default-features --features encryption",
            toolchain: "stable",
        },
        TestSuite {
            cargo_args: "--package bonsaidb-server --no-default-features --features encryption,compression",
            toolchain: "stable",
        },
        TestSuite {
            cargo_args: "--package bonsaidb-server --no-default-features --features compression",
            toolchain: "stable",
        },
        TestSuite {
            cargo_args: "--package bonsaidb-server --no-default-features --features websockets",
            toolchain: "stable",
        },
        TestSuite {
            cargo_args: "--package bonsaidb-server --no-default-features --features acme",
            toolchain: "stable",
        },
        TestSuite {
            cargo_args: "--package bonsaidb --no-default-features --features server,client,test-util",
            toolchain: "stable",
        },
        TestSuite {
            cargo_args: "--package bonsaidb --no-default-features --features server,client,test-util,token-authentication,websockets",
            toolchain: "stable",
        },
        TestSuite {
            cargo_args: "--package bonsaidb --no-default-features --features server,client,test-util,password-hashing,websockets",
            toolchain: "stable",
        },
        TestSuite {
            cargo_args: "--package bonsaidb --no-default-features --features server,client,test-util,password-hashing,acme",
            toolchain: "stable",
        },
        TestSuite {
            cargo_args: "--package bonsaidb --no-default-features --features server,client,test-util,password-hashing,compression",
            toolchain: "stable",
        },
        TestSuite {
            cargo_args: "--package bonsaidb --no-default-features --features server,client,test-util,password-hashing,encryption",
            toolchain: "stable",
        },
        TestSuite {
            cargo_args:
                "--package bonsaidb --no-default-features --features server,client,test-util,acme,websockets",
            toolchain: "stable",
        },
        TestSuite {
            cargo_args:
                "--package bonsaidb --no-default-features --features server,client,test-util,acme,websockets,password-hashing",
            toolchain: "stable",
        },
        TestSuite {
            cargo_args: "--package bonsaidb-files --no-default-features",
            toolchain: "stable",
        },
    ]
}

fn generate_test_matrix_output() -> anyhow::Result<()> {
    let stdout = stdout();
    let mut stdout = stdout.lock();
    stdout.write_all(b"::set-output name=test-matrix::")?;
    stdout.write_all(&serde_json::to_vec(all_tests())?)?;
    stdout.write_all(b"\n")?;
    Ok(())
}

fn run_all_tests(fail_on_warnings: bool) -> anyhow::Result<()> {
    let executing_dir = current_dir()?;
    let mut all_tests = all_tests().iter().enumerate().collect::<Vec<_>>();

    if let Some(last_index) = last_succeeded_index() {
        let recently_finished = all_tests.drain(..last_index + 1).collect::<Vec<_>>();
        all_tests.extend(recently_finished);
    }

    for (index, test) in all_tests {
        println!("Running clippy for {}", test.cargo_args);
        let mut clippy = Cmd::new("cargo");
        let mut clippy = clippy
            .arg("clippy")
            .arg("--all-targets")
            .env("RUST_TOOLCHAIN", test.toolchain);

        for arg in test.cargo_args.split(' ') {
            clippy = clippy.arg(arg);
        }

        if fail_on_warnings {
            clippy = clippy.arg("--").arg("-D").arg("warnings");
        }

        clippy.run()?;

        println!("Running tests for {}", test.cargo_args);
        let mut cargo = Cmd::new("cargo");
        let mut cargo = cargo
            .arg("test")
            .arg("--all-targets")
            .env("RUST_TOOLCHAIN", test.toolchain);

        for arg in test.cargo_args.split(' ') {
            cargo = cargo.arg(arg);
        }
        cargo.run()?;
        set_last_succeeded_index(index);
    }

    println!("Running clippy for wasm32 client");
    set_current_dir(executing_dir)?;
    let mut clippy = Cmd::new("cargo");
    let mut clippy = clippy.args([
        "clippy",
        "--target",
        "wasm32-unknown-unknown",
        "--target-dir",
        "target/wasm",
        "--package",
        "bonsaidb-client",
    ]);
    if fail_on_warnings {
        clippy = clippy.arg("--").arg("-D").arg("warnings");
    }

    clippy.run()?;

    println!("Generating docs");
    run!("cargo", "doc", "--all-features", "--no-deps")?;
    Ok(())
}

fn set_last_succeeded_index(index: usize) {
    drop(std::fs::write(
        temp_dir().join("bonsaidb-test-all-index"),
        index.to_string().as_bytes(),
    ));
}

fn last_succeeded_index() -> Option<usize> {
    std::fs::read_to_string(temp_dir().join("bonsaidb-test-all-index"))
        .ok()
        .and_then(|contents| usize::from_str(&contents).ok())
}
