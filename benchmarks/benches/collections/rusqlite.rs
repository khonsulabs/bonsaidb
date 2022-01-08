use std::path::Path;

use criterion::{measurement::WallTime, BenchmarkGroup, BenchmarkId};
use rusqlite::{params, Connection};
use ubyte::ToByteUnit;

use crate::collections::ResizableDocument;

pub(super) fn save_documents(group: &mut BenchmarkGroup<WallTime>, doc: &ResizableDocument) {
    group.bench_function(BenchmarkId::new("rusqlite", doc.data.len().bytes()), |b| {
        let file = Path::new("benches-collections.sqlite3");
        if file.exists() {
            std::fs::remove_file(file).unwrap();
        }
        // For fair testing, this needs to use ACID-compliant settings that a
        // user would use in production. While a WAL might be used in
        // production, it alters more than just insert performance. A more
        // complete benchmark which includes both inserts and queries would be
        // better to compare roots against sqlite's WAL performance.
        let sqlite = Connection::open(file).unwrap();
        // Sets the journal to what seems to be the most optimal, safe setting
        // for @ecton. See:
        // https://www.sqlite.org/pragma.html#pragma_journal_mode
        sqlite
            .pragma_update(None, "journal_mode", &"TRUNCATE")
            .unwrap();
        // Sets synchronous to NORMAL, which "should" be safe and provides
        // better performance. See:
        // https://www.sqlite.org/pragma.html#pragma_synchronous
        sqlite
            .pragma_update(None, "synchronous", &"NORMAL")
            .unwrap();
        sqlite
            .execute(
                "create table save_documents (id integer primary key autoincrement, data BLOB)",
                [],
            )
            .unwrap();

        let mut prepared = sqlite
            .prepare("insert into save_documents (data) values (?)")
            .unwrap();
        b.iter(|| {
            prepared.execute(params![doc.data]).unwrap();
        });
    });

    // TODO bench read performance
    // TODO bench read + write performance (with different numbers of readers/writers)
    // TODO (once supported) bench batch saving
}
