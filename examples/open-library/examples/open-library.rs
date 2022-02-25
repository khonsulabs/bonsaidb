use std::{self, collections::BTreeMap, fs::File, io::Read, mem, path::Path};

use bonsaidb::{
    core::{
        connection::{Bound, Connection, Range},
        schema::{Collection, Schema, SerializedCollection},
        transaction::{Operation, Transaction},
    },
    local::{
        config::{Builder, Compression, StorageConfiguration},
        Database,
    },
};
use futures::{Future, StreamExt};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use time::{Date, Month};

#[derive(Debug, Schema)]
#[schema(name = "open-library", collections = [Author, Work, Edition, Rating, ReadingLog])]
struct OpenLibrary;

#[derive(Debug, Serialize, Deserialize, Collection)]
#[collection(name = "authors", primary_key = String, natural_id = |author: &Self| Some(author.key.clone()))]
struct Author {
    pub key: String,
    pub name: Option<String>,
    #[serde(default)]
    pub alternate_names: Vec<String>,
    pub bio: Option<TypedValue>,
    pub birth_date: Option<String>,
    pub death_date: Option<String>,
    pub location: Option<String>,
    pub date: Option<String>,
    pub entity_type: Option<String>,
    pub fuller_name: Option<String>,
    pub personal_name: Option<String>,
    pub title: Option<String>,
    #[serde(default)]
    pub photos: Vec<Option<i64>>,
    #[serde(default)]
    pub links: Vec<Link>,
    pub created: Option<TypedValue>,
    pub last_modified: TypedValue,
}

#[derive(Debug, Serialize, Deserialize, Collection)]
#[collection(name = "editions", primary_key = String, natural_id = |edition: &Self| Some(edition.key.clone()))]
struct Edition {
    pub key: String,
    pub title: Option<String>,
    pub subtitle: Option<String>,
    #[serde(default)]
    pub authors: Vec<Reference>,
    #[serde(default)]
    pub works: Vec<Reference>,
    #[serde(default)]
    pub identifiers: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    pub isbn_10: Vec<String>,
    #[serde(default)]
    pub isbn_13: Vec<String>,
    #[serde(default)]
    pub lccn: Vec<String>,
    #[serde(default)]
    pub oclc_numbers: Vec<String>,
    #[serde(default)]
    pub covers: Vec<Option<i64>>,
    #[serde(default)]
    pub links: Vec<Link>,
    pub by_statement: Option<String>,
    pub weight: Option<String>,
    pub edition_name: Option<String>,
    pub number_of_pages: Option<i32>,
    pub pagination: Option<String>,
    pub physical_dimensions: Option<String>,
    pub physical_format: Option<String>,
    pub publish_country: Option<String>,
    pub publish_date: Option<String>,
    #[serde(default)]
    pub publish_places: Vec<String>,
    #[serde(default)]
    pub publishers: Vec<String>,
    #[serde(default)]
    pub contributions: Vec<String>,
    #[serde(default)]
    pub dewey_decimal_class: Vec<String>,
    #[serde(default)]
    pub genres: Vec<String>,
    #[serde(default)]
    pub lc_classifications: Vec<String>,
    #[serde(default)]
    pub other_titles: Vec<String>,
    #[serde(default)]
    pub series: Vec<String>,
    #[serde(default)]
    pub source_records: Vec<Option<String>>,
    #[serde(default)]
    pub subjects: Vec<String>,
    #[serde(default)]
    pub work_titles: Vec<String>,
    #[serde(default)]
    pub table_of_contents: Vec<serde_json::Value>,
    pub description: Option<TypedValue>,
    pub first_sentence: Option<TypedValue>,
    pub notes: Option<TypedValue>,
    pub created: Option<TypedValue>,
    pub last_modified: TypedValue,
}

#[derive(Debug, Serialize, Deserialize, Collection)]
#[collection(name = "works", primary_key = String, natural_id = |work: &Self| Some(work.key.clone()))]
struct Work {
    pub key: String,
    pub title: Option<String>,
    pub subtitle: Option<String>,
    #[serde(default)]
    pub authors: Vec<AuthorRole>,
    #[serde(default)]
    pub covers: Vec<Option<i64>>,
    #[serde(default)]
    pub links: Vec<Link>,
    pub id: Option<i64>,
    #[serde(default)]
    pub lc_classifications: Vec<String>,
    #[serde(default)]
    pub subjects: Vec<String>,
    pub first_publish_date: Option<String>,
    pub description: Option<TypedValue>,
    pub notes: Option<TypedValue>,
    pub created: Option<TypedValue>,
    pub last_modified: TypedValue,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
enum TypedValue {
    TypeValue { r#type: String, value: String },
    Value(String),
}

#[derive(Serialize, Deserialize, Debug)]
struct AuthorRole {
    pub role: Option<String>,
    pub r#as: Option<String>,
    pub author: Option<ExternalKey>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
enum Reference {
    Typed(TypedReference),
    Key(String),
}

#[derive(Serialize, Deserialize, Debug)]
struct TypedReference {
    pub r#type: Option<String>,
    pub key: ExternalKey,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
enum ExternalKey {
    Tagged { key: String },
    Untagged(String),
}

#[derive(Serialize, Deserialize, Debug)]
struct Link {
    pub title: Option<String>,
    pub url: String,
}

#[derive(Debug, Serialize, Deserialize, Collection)]
#[collection(name = "ratings")]
struct Rating {
    pub work_key: String,
    pub edition_key: String,
    pub date: Date,
    pub rating: u8,
}

impl TryFrom<Vec<String>> for Rating {
    type Error = anyhow::Error;

    fn try_from(fields: Vec<String>) -> Result<Self, Self::Error> {
        if fields.len() != 4 {
            anyhow::bail!("expected 4 fields, got {:?}", fields);
        }

        let mut fields = fields.into_iter();
        let work_key = fields.next().unwrap();
        let edition_key = fields.next().unwrap();
        let rating = fields.next().unwrap();
        let rating = rating.parse::<u8>()?;
        let date = fields.next().unwrap();
        let mut date_parts = date.split('-');
        let year = date_parts.next().unwrap().to_owned();
        let year = year.parse::<i32>()?;
        let month = date_parts.next().unwrap().to_owned();
        let month = month.parse::<u8>()?;
        let month = Month::try_from(month)?;
        let day = date_parts.next().unwrap().to_owned();
        let day = day.parse::<u8>()?;
        let date = Date::from_calendar_date(year, month, day)?;

        Ok(Self {
            work_key,
            edition_key,
            date,
            rating,
        })
    }
}

#[derive(Debug, Serialize, Deserialize, Collection)]
#[collection(name = "reading-logs")]
struct ReadingLog {
    pub work_key: String,
    pub edition_key: String,
    pub date: Date,
    pub shelf: String,
}

fn parse_tsv(
    path: impl AsRef<Path> + Send + Sync,
    output: flume::Sender<Vec<String>>,
) -> anyhow::Result<()> {
    let mut file = File::open(path)?;
    let mut buffer = vec![0; 16192];
    // let mut file = gzip::Decoder::new(file)?;
    let mut current_record = vec![Vec::new()];
    loop {
        let bytes_read = file.read(&mut buffer)?;
        if bytes_read == 0 {
            // TODO handle dropping the last record?
            break;
        }
        for &ch in &buffer[..bytes_read] {
            match ch {
                b'\t' => {
                    // Next field
                    current_record.push(Vec::new());
                }
                b'\r' => {}
                b'\n' => {
                    // Swap an empty record into the current_record, and send
                    // the record to the output channel.
                    let mut record = vec![Vec::new()];
                    mem::swap(&mut record, &mut current_record);
                    // Each field should be UTF-8
                    let record = record
                        .into_iter()
                        .map(String::from_utf8)
                        .collect::<Result<Vec<String>, _>>()?;
                    output.send(record)?;
                }
                other => {
                    current_record.last_mut().unwrap().push(other);
                }
            }
        }
    }

    Ok(())
}

async fn import_ratings(database: &Database) -> anyhow::Result<()> {
    import_from_tsv(
        "./examples/open-library/ol_dump_ratings.txt",
        database,
        |records, database| async move {
            let mut tx = Transaction::new();
            for record in records {
                tx.push(Operation::push_serialized::<Rating>(&Rating::try_from(
                    record,
                )?)?);
            }
            let inserted = tx.operations.len();
            database.apply_transaction(tx).await?;
            Ok(inserted)
        },
    )
    .await
}

async fn overwrite_serialized<C: SerializedCollection>(
    tx: &mut Transaction,
    json: &str,
) -> anyhow::Result<()>
where
    C::Contents: DeserializeOwned,
{
    match serde_json::from_str::<C::Contents>(json) {
        Ok(contents) => {
            tx.push(Operation::overwrite_serialized::<C>(
                C::natural_id(&contents).unwrap(),
                &contents,
            )?);
            Ok(())
        }
        Err(err) => {
            anyhow::bail!("Error parsing json {}: {}", err, json);
        }
    }
}

async fn import_primary_data(database: &Database) -> anyhow::Result<()> {
    import_from_tsv(
        "./examples/open-library/ol_dump_all.txt",
        database,
        |records, database| async move {
            let mut tx = Transaction::new();
            for record in &records {
                match record[0].as_str() {
                    "/type/author" => overwrite_serialized::<Author>(&mut tx, &record[4]).await?,
                    "/type/edition" => overwrite_serialized::<Edition>(&mut tx, &record[4]).await?,
                    "/type/work" => overwrite_serialized::<Work>(&mut tx, &record[4]).await?,
                    _ => {}
                }
            }
            let inserted = tx.operations.len();
            database.apply_transaction(tx).await?;
            Ok(inserted)
        },
    )
    .await
}

async fn import_from_tsv<
    Callback: Fn(Vec<Vec<String>>, Database) -> Fut + 'static,
    Fut: Future<Output = anyhow::Result<usize>>,
>(
    path: &'static str,
    database: &Database,
    callback: Callback,
) -> anyhow::Result<()> {
    const CHUNK_SIZE: usize = 500_000;
    let (sender, receiver) = flume::bounded(CHUNK_SIZE * 2);
    std::thread::spawn(move || parse_tsv(path, sender));

    let mut inserted = 0;
    let mut record_stream = receiver.into_stream().chunks(CHUNK_SIZE);
    while let Some(records) = record_stream.next().await {
        inserted += callback(records, database.clone()).await?;
        println!("Imported records: {}", inserted);
    }

    Ok(())
}

/// A paginated version of counting entries. For when you have more data stored
/// than you have ram...
// TODO implement an actual count function to avoid loading all the documents
// https://github.com/khonsulabs/bonsaidb/issues/176
async fn count<C>(db: &Database) -> anyhow::Result<usize>
where
    C: Collection + Unpin,
    C::PrimaryKey: Default + Unpin,
{
    let mut last_id = <C::PrimaryKey as Default>::default();
    let mut count = 0;
    loop {
        let batch = db
            .collection::<C>()
            .list(Range {
                start: Bound::Excluded(last_id),
                end: Bound::Unbounded,
            })
            .limit(1_000_000)
            .await?;
        match batch.len() {
            0 => break,
            batch_length => {
                count += batch_length;
            }
        }

        last_id = batch.last().unwrap().header.id.deserialize()?;
    }
    Ok(count)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db = Database::open::<OpenLibrary>(
        StorageConfiguration::new("open-library.bonsaidb").default_compression(Compression::Lz4),
    )
    .await?;

    let primary_import = import_primary_data(&db);
    let ratings_import = import_ratings(&db);
    tokio::try_join!(primary_import, ratings_import)?;

    println!("Compacting");
    db.compact().await?;
    println!("Done compacting");

    println!("Total authors: {}", count::<Author>(&db).await?);
    println!("Total works: {}", count::<Work>(&db).await?);
    println!("Total editions: {}", count::<Edition>(&db).await?);
    println!("Total ratings: {}", count::<Rating>(&db).await?);

    Ok(())
}

#[test]
fn runs() {
    main().unwrap()
}
