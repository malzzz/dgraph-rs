use std::collections::HashMap;

use grpc::api::TxnContext;
use grpcio::{Result as GrpcResult};
use protobuf::{RepeatedField, SingularPtrField};

use client::Dgraph;
use grpc::api::{Assigned, Mutation, Request, Response};
use error::{Error, ErrorKind};

pub struct Transaction<'a> {
    context: TxnContext,
    finished: bool,
    mutated: bool,
    dg: &'a mut Dgraph
}

impl<'a> Transaction<'a> {
    pub fn new(d: &'a mut Dgraph) -> Self {
        let mut txn_context = TxnContext::new();
        txn_context.set_lin_read(d.get_lin_read());

        Transaction {
            context: txn_context,
            finished: false,
            mutated: false,
            dg: d
        }
    }

    pub fn query(&mut self, q: &str) -> Result<Response, Error> {
        self.query_with_vars(q, HashMap::new())
    }

    pub fn query_with_vars(&mut self, q: &str, vars: HashMap<String, String>)
        -> Result<Response, Error>
    {
        if self.finished {
            return Err(ErrorKind::Finished.into())
        }

        let mut req = Request::new();
        req.set_vars(vars);
        req.set_query(q.to_string());
        req.set_start_ts(self.context.start_ts);
        req.set_lin_read(self.context.get_lin_read().clone());

        let dc = self.dg.any_client();

        match dc.query(&req) {
            Ok(resp) => {
                if let Err(e) = self.merge_context(&resp.get_txn()) {
                    Err(ErrorKind::GrpcError(e.to_string()).into())
                } else {
                    Ok(resp)
                }
            },
            Err(e) => Err(ErrorKind::GrpcError(e.to_string()).into())
        }
    }

    pub fn merge_context(&mut self, src: &TxnContext) -> Result<(), Error> {
        if src.lin_read.is_none() {
            return Ok(())
        }

        super::merge_lin_reads(self.context.mut_lin_read(), &src.get_lin_read());
        self.dg.merge_lin_read(&src.get_lin_read());

        if self.context.start_ts == 0 {
            self.context.start_ts = src.start_ts;
        }

        if self.context.start_ts != src.start_ts {
            return Err(ErrorKind::TransactionTs(self.context.start_ts, src.start_ts).into())
        }

        let mut merged_keys = self.context.keys.clone().into_vec();
        merged_keys.append(&mut src.keys.clone().into_vec());

        self.context.set_keys(RepeatedField::from_vec(merged_keys));
        Ok(())
    }

    pub fn discard(&mut self) -> Result<(), Error> {
        if self.finished {
            return Ok(())
        }

        self.finished = true;

        if !self.mutated {
            return Ok(())
        }

        self.context.set_aborted(true);
        let dc = self.dg.any_client();
        match dc.commit_or_abort(&self.context) {
            Ok(_) => Ok(()),
            Err(e) => Err(ErrorKind::Discard(e.to_string()).into())
        }
    }

    pub fn mutate(&mut self, mu: &mut Mutation) -> Result<Assigned, Error> {
        if self.finished {
            return Err(ErrorKind::Finished.into())
        }
        self.mutated = true;
        mu.set_start_ts(self.context.start_ts);

        let dc = self.dg.any_client();
        match dc.mutate(&mu) {
            Err(e) => {
                let _: Result<(), Error> = self.discard();
                Err(ErrorKind::Mutation(e.to_string()).into())
            },
            Ok(ag) => {
                if mu.commit_now {
                    self.finished = true;
                }
                match self.merge_context(ag.get_context()) {
                    Err(e) => Err(e),
                    Ok(()) => Ok(ag)
                }
            }
        }
    }

    pub fn commit(&mut self) -> Result<(), Error> {
        if self.finished {
            return Err(ErrorKind::Finished.into())
        }
        self.finished = true;

        if !self.mutated {
            return Ok(())
        }
        let dc = self.dg.any_client();
        match dc.commit_or_abort(&self.context) {
            Err(e) => Err(ErrorKind::Commit(e.to_string()).into()),
            Ok(_) => Ok(())
        }
    }

}