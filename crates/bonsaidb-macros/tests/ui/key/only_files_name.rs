use bonsaidb::files::FileConfig;

#[derive(FileConfig)]
#[file_config(files_name = "authority")]
struct Test;

fn main() {}
