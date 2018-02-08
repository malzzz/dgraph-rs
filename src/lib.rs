extern crate futures;
extern crate grpcio;
extern crate parking_lot;
extern crate protobuf;
extern crate rand;
extern crate serde_json;
extern crate serde;

#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate error_chain;

mod client;
pub mod error;
pub mod grpc;
mod transaction;

pub use self::transaction::Transaction;
pub use client::Dgraph;
pub use grpc::api::{Mutation, Operation};
use grpc::api::LinRead;

pub fn merge_lin_reads(dst: &mut LinRead, src: &LinRead) {
    if src.ids.is_empty() {
        return
    }

    let mut dst_ids = dst.get_ids().clone();
    for (gid, sid) in src.get_ids().iter() {
        let has = dst_ids.contains_key(&gid);
        let did = dst_ids.get(&gid).map(|x| x >= sid).unwrap_or(false);

        if has && did {
            continue
        } else {
            *dst_ids.entry(*gid).or_insert(sid.clone()) = sid.clone();
        }
    }
}
