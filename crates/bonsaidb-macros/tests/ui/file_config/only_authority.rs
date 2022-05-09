use bonsaidb::files::FileConfig;

#[derive(FileConfig)]
#[file_config(authority = "authority")]
struct Test;

fn main() {}
