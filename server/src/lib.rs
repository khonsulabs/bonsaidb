// Server data folder:
// data/
//   dbs/
//     foobar.pliantdb/
//     config.pliantdb/
//       databases
//

mod admin;
mod error;
mod hosted;
mod server;

pub use self::{error::Error, server::Server};

#[cfg(test)]
mod tests;
