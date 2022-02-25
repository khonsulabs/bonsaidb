# open-library

Data source: <https://openlibrary.org/developers/dumps>

Download:

- all types dump
- ratings dump
- reading log dump

Decompress the files and place them next to this README, named:

- ol_dump_all.txt
- ol_dump_ratingx.txt
- ol_dump_reading-log.txt

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
