use std::borrow::Cow;

use pliantdb_core::{
    schema::Collection,
    test_util::{self, Basic, TestDirectory},
    transaction::{self, Operation, OperationResult, Transaction},
};
use pliantdb_networking::{DatabaseRequest, DatabaseResponse, Request, Response};

use crate::{hosted::Database, test_util::initialize_basic_server, Server};

#[tokio::test(flavor = "multi_thread")]
async fn simple_test() -> anyhow::Result<()> {
    let test_dir = TestDirectory::new("simple-test");
    let server = initialize_basic_server(test_dir.as_ref()).await?;
    let db = server.database::<Basic>("tests").await?;
    test_util::store_retrieve_update_delete_tests(&db).await
}

struct TestHarness {
    _directory: TestDirectory,
    server: Server,
}

impl TestHarness {
    pub async fn new(name: &str) -> anyhow::Result<Self> {
        let directory = TestDirectory::new(name);
        let server = initialize_basic_server(directory.as_ref()).await?;
        Ok(Self {
            _directory: directory,
            server,
        })
    }

    pub async fn connect<'a, 'b>(&'a self) -> anyhow::Result<Database<'a, 'b, Basic>> {
        let db = self.server.database::<Basic>("tests").await?;
        Ok(db)
    }
}

pliantdb_core::define_connection_test_suite!(TestHarness);

// TODO test creating/deleting databases/listing
// TODO test available schemas

#[tokio::test(flavor = "multi_thread")]
async fn handle_request_tests() -> anyhow::Result<()> {
    let test_dir = TestDirectory::new("handle-requests");
    let server = initialize_basic_server(test_dir.as_ref()).await?;
    let mut transaction = Transaction::default();
    transaction.push(Operation {
        collection: Basic::collection_id(),
        command: transaction::Command::Insert {
            contents: Cow::default(),
        },
    });
    let results = server
        .handle_request(Request::Database {
            database: Cow::from("tests"),
            request: DatabaseRequest::ApplyTransaction { transaction },
        })
        .await?;
    let id = if let Response::Database(DatabaseResponse::TransactionResults(results)) = &results {
        assert_eq!(results.len(), 1);
        if let OperationResult::DocumentUpdated { header, collection } = &results[0] {
            assert_eq!(collection, &Basic::collection_id());
            header.id
        } else {
            panic!("unexpected operation result from insert: {:?}", results[0])
        }
    } else {
        panic!("unexpected result from apply_transaction {:?}", results)
    };

    assert!(matches!(
        server
            .handle_request(Request::Database {
                database: Cow::from("tests"),
                request: DatabaseRequest::Get {
                    collection: Basic::collection_id(),
                    id
                }
            })
            .await,
        Ok(Response::Database(DatabaseResponse::Documents(_)))
    ));

    Ok(())
}
