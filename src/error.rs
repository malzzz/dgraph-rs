#![allow(unknown_lints)]
#![allow(missing_docs)]

use std::io;

error_chain! {
  foreign_links {
    Io(io::Error);
  }
  errors {
    Finished {
        description("transaction finished"),
        display("Transaction has already been committed or discarded")
    }
    TransactionTs(txn: u64, src: u64) {
        description("start ts"),
        display("`start_ts` of transaction ({}) does not match that of source ({})", txn, src)
    }
    GrpcError(e: String) {
        description("grpc error"),
        display("gRPC error: {}", e)
    }
    Discard(e: String) {
        description("discard error"),
        display("Error discarding transaction: {}", e)
    }
    Mutation(e: String) {
        description("mutation error"),
        display("Mutation error: {}", e)
    }
    Commit(e: String) {
        description("commit error"),
        display("Commit error: {}", e)
    }
  }
}

