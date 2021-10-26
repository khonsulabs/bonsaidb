use std::time::Duration;

use actionable::{Action, ActionNameList, Permissions, ResourceName};
use bonsaidb::{
    client::{Client, RemoteDatabase},
    core::{
        admin::{Admin, PermissionGroup},
        connection::ServerConnection,
        permissions::Statement,
        schema::Collection,
        test_util::{BasicSchema, HarnessTest, TestDirectory},
    },
    server::test_util::{initialize_basic_server, BASIC_SERVER_NAME},
};
use bonsaidb_core::{
    permissions::bonsai::{BonsaiAction, ServerAction},
    schema::InsertError,
};
use bonsaidb_server::{Configuration, DefaultPermissions, Server};
use fabruic::Certificate;
use once_cell::sync::Lazy;
use tokio::sync::Mutex;
use url::Url;

async fn initialize_shared_server() -> Certificate {
    static CERTIFICATE: Lazy<Mutex<Option<Certificate>>> = Lazy::new(|| Mutex::new(None));
    let mut certificate = CERTIFICATE.lock().await;
    if certificate.is_none() {
        let (sender, receiver) = flume::bounded(1);
        std::thread::spawn(|| run_shared_server(sender));

        *certificate = Some(receiver.recv_async().await.unwrap());
        // Give the server time to start listening
        tokio::time::sleep(Duration::from_millis(1000)).await;
    }

    certificate.clone().unwrap()
}

fn run_shared_server(certificate_sender: flume::Sender<Certificate>) -> anyhow::Result<()> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async move {
        let directory = TestDirectory::new("shared-server");
        let server = initialize_basic_server(directory.as_ref()).await.unwrap();
        certificate_sender
            .send(server.certificate().await.unwrap())
            .unwrap();

        #[cfg(feature = "websockets")]
        {
            let task_server = server.clone();
            tokio::spawn(async move {
                task_server
                    .listen_for_websockets_on("localhost:6001")
                    .await
                    .unwrap();
            });
        }

        server.listen_on(6000).await.unwrap();
    });

    Ok(())
}

#[cfg(feature = "websockets")]
mod websockets {
    use super::*;

    struct WebsocketTestHarness {
        client: Client,
        url: Url,
        db: RemoteDatabase<BasicSchema>,
    }

    impl WebsocketTestHarness {
        pub async fn new(test: HarnessTest) -> anyhow::Result<Self> {
            initialize_shared_server().await;
            let url = Url::parse("ws://localhost:6001")?;
            let client = Client::new(url.clone()).await?;

            let dbname = format!("websockets-{}", test);
            client.create_database::<BasicSchema>(&dbname).await?;
            let db = client.database::<BasicSchema>(&dbname).await?;

            Ok(Self { client, url, db })
        }

        pub const fn server_name() -> &'static str {
            "websocket"
        }

        pub fn server(&self) -> &'_ Client {
            &self.client
        }

        pub async fn connect<'a, 'b>(&'a self) -> anyhow::Result<RemoteDatabase<BasicSchema>> {
            Ok(self.db.clone())
        }

        #[allow(dead_code)] // We will want this in the future but it's currently unused
        pub async fn connect_with_permissions(
            &self,
            permissions: Vec<Statement>,
            label: &str,
        ) -> anyhow::Result<RemoteDatabase<BasicSchema>> {
            let client = Client::new(self.url.clone()).await?;
            assume_permissions(client, label, self.db.name(), permissions).await
        }

        pub async fn shutdown(&self) -> anyhow::Result<()> {
            Ok(())
        }
    }

    bonsaidb_core::define_connection_test_suite!(WebsocketTestHarness);

    bonsaidb_core::define_pubsub_test_suite!(WebsocketTestHarness);
    bonsaidb_core::define_kv_test_suite!(WebsocketTestHarness);
}

mod bonsai {
    use super::*;
    struct BonsaiTestHarness {
        client: Client,
        url: Url,
        certificate: Certificate,
        db: RemoteDatabase<BasicSchema>,
    }

    impl BonsaiTestHarness {
        pub async fn new(test: HarnessTest) -> anyhow::Result<Self> {
            let certificate = initialize_shared_server().await;

            let url = Url::parse(&format!(
                "bonsaidb://localhost:6000?server={}",
                BASIC_SERVER_NAME
            ))?;
            let client = Client::build(url.clone())
                .with_certificate(certificate.clone())
                .finish()
                .await?;

            let dbname = format!("bonsai-{}", test);
            client.create_database::<BasicSchema>(&dbname).await?;
            let db = client.database::<BasicSchema>(&dbname).await?;

            Ok(Self {
                client,
                url,
                certificate,
                db,
            })
        }

        pub fn server_name() -> &'static str {
            "bonsai"
        }

        pub fn server(&self) -> &'_ Client {
            &self.client
        }

        pub async fn connect<'a, 'b>(&'a self) -> anyhow::Result<RemoteDatabase<BasicSchema>> {
            Ok(self.db.clone())
        }

        #[allow(dead_code)] // We will want this in the future but it's currently unused
        pub async fn connect_with_permissions(
            &self,
            statements: Vec<Statement>,
            label: &str,
        ) -> anyhow::Result<RemoteDatabase<BasicSchema>> {
            let client = Client::build(self.url.clone())
                .with_certificate(self.certificate.clone())
                .finish()
                .await?;
            assume_permissions(client, label, self.db.name(), statements).await
        }

        pub async fn shutdown(&self) -> anyhow::Result<()> {
            Ok(())
        }
    }

    bonsaidb_core::define_connection_test_suite!(BonsaiTestHarness);
    bonsaidb_core::define_pubsub_test_suite!(BonsaiTestHarness);
    bonsaidb_core::define_kv_test_suite!(BonsaiTestHarness);
}

#[allow(dead_code)] // We will want this in the future but it's currently unused
async fn assume_permissions(
    connection: Client,
    label: &str,
    database_name: &str,
    statements: Vec<Statement>,
) -> anyhow::Result<RemoteDatabase<BasicSchema>> {
    let username = format!("{}-{}", database_name, label);
    match connection.create_user(&username).await {
        Ok(user_id) => {
            // Set the user's password. This uses OPAQUE to ensure the password
            // never leaves the machine that executes `set_user_password_str`.
            connection
                .set_user_password_str(&username, "hunter2")
                .await
                .unwrap();

            // Create an administrators permission group, or get its ID if it already existed.
            let admin = connection.database::<Admin>("admin").await?;
            let administrator_group_id = match (PermissionGroup {
                name: String::from(label),
                statements,
            }
            .insert_into(&admin)
            .await)
            {
                Ok(doc) => doc.header.id,
                Err(InsertError {
                    error:
                        bonsaidb_core::Error::UniqueKeyViolation {
                            existing_document_id,
                            ..
                        },
                    ..
                }) => existing_document_id,
                Err(other) => anyhow::bail!(other),
            };

            // Make our user a member of the administrators group.
            connection
                .add_permission_group_to_user(user_id, administrator_group_id)
                .await
                .unwrap();
        }
        Err(bonsaidb_core::Error::UniqueKeyViolation { .. }) => {}
        Err(other) => anyhow::bail!(other),
    };

    connection
        .login_with_password_str(&username, "hunter2", None)
        .await
        .unwrap();

    Ok(connection.database::<BasicSchema>(database_name).await?)
}

#[tokio::test]
#[ignore = "https://github.com/khonsulabs/custodian/issues/9"]
async fn authenticated_permissions_test() -> anyhow::Result<()> {
    let database_path = TestDirectory::new("authenticated-permissions");
    let server = Server::open(
        &database_path,
        Configuration {
            default_permissions: DefaultPermissions::Permissions(Permissions::from(vec![
                Statement {
                    resources: vec![ResourceName::any()],
                    actions: ActionNameList::List(vec![
                        BonsaiAction::Server(ServerAction::Connect).name(),
                        BonsaiAction::Server(ServerAction::LoginWithPassword).name(),
                    ]),
                },
            ])),
            authenticated_permissions: DefaultPermissions::AllowAll,
            ..Configuration::default()
        },
    )
    .await?;
    server
        .install_self_signed_certificate("authenticated-permissions-test", false)
        .await?;
    let certificate = server.certificate().await?;

    server.create_user("ecton").await?;
    server.set_user_password_str("ecton", "hunter2").await?;
    tokio::spawn(async move {
        server.listen_on(6002).await?;
        Result::<(), anyhow::Error>::Ok(())
    });
    // Give the server time to listen
    tokio::time::sleep(Duration::from_millis(10)).await;

    let url = Url::parse("bonsaidb://localhost:6002")?;
    let client = Client::build(url)
        .with_certificate(certificate)
        .finish()
        .await?;
    match client.create_user("otheruser").await {
        Err(bonsaidb_core::Error::PermissionDenied(_)) => {}
        _ => unreachable!("should not have permission to create another user before logging in"),
    }
    client
        .login_with_password_str("ecton", "hunter2", None)
        .await?;
    client
        .create_user("otheruser")
        .await
        .expect("should be able to create user after logging in");

    Ok(())
}
