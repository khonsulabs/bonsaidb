# open-library

This example was created to begin testing BonsaiDb with a larger dataset. The
dataset is very large (50gb uncompressed), and the process of building the
databases requires significantly more free space.

Data source: <https://openlibrary.org/developers/dumps>

Download:

- all types dump
- ratings dump
- reading log dump

Decompress the files and place them next to this README, named:

- ol_dump_all.txt
- ol_dump_ratingx.txt
- ol_dump_reading-log.txt

## Commands

For all commands, `-z` enables LZ4 compression and stores the database to a separate path.

### Importing data

```text
cargo run -p open-library --example open-library --release -- -z import
```

### Compacting

```text
cargo run -p open-library --example open-library --release -- -z compact
```

### Finding an Author

```text
$ cargo run -p open-library --example open-library --release -- -z author OL1394865A
Name: Brandon Sanderson
Biography:
I'm Brandon Sanderson, and I write stories of the fantastic: fantasy, science fiction, and thrillers.
...
Works:
/works/OL14909011W: Alcatraz versus the Shattered Lens
/works/OL15358691W: The Way of Kings
...
/works/OL5738149W: Warbreaker
...
```

### Finding a Work (Book)

```text
$ cargo run -p open-library --example open-library --release -- -z work OL5738149W
Title: Warbreaker
Editions:
/books/OL23147839M: Warbreaker
```

### Finding an Edition

```text
$ cargo run -p open-library --example open-library --release -- -z edition OL23147839M
Title: Warbreaker
Works:
/works/OL5738149W: Warbreaker
```

## Data Sizes

Editions, Authors, and Works come from the "all types" export. This file contains extra records that
are discarded, so there isn't much point to comparing this file's total size
against the database sizes.

The numbers from below were generated using the 2021-11-30 export.

| File                    | TSV + JSON |     Gzip |
|-------------------------|------------|----------|
| ol_dump_all.txt         |   51,447mb | 10,172mb |
| ol_dump_ratings.txt     |       11mb |      3mb |
| ol_dump_reading-log.txt |      162mb |     28mb |

All data inserted was batched in transactions of 500k entries.

| Collection | Records    | LZ4 Uncompacted | LZ4 Compacted | Uncompacted | Compacted |
|------------|------------|-----------------|---------------|-------------|-----------|
| Authors    |  9,145,605 |       13,316mb |        3,675mb |    23,508mb |   4,159mb |
| Editions   | 33,102,390 |       74,459mb |       33,155mb |   118,330mb |  38,372mb |
| Ratings    |    234,571 |           59mb |           54mb |        65mb |      56mb |
| Works      | 24,010,896 |       40,443mb |       12,082mb |    71,601mb |  13,565mb |
