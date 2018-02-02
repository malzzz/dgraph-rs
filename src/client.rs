use std::sync::Arc;

use grpcio::Error;
use parking_lot::Mutex;
use rand;
use rand::distributions::{IndependentSample, Range};
use protobuf::SingularPtrField;

use grpc::api::{LinRead, Mutation, NQuad, Operation, TxnContext, Value, Value_oneof_val};
use grpc::api_grpc::DgraphClient;

pub struct Dgraph {
    dc: Vec<Arc<DgraphClient>>,
    lin_read: Arc<Mutex<LinRead>>
}

impl Dgraph {
    pub fn new(clients: Vec<Arc<DgraphClient>>) -> Dgraph {
        Dgraph {
            dc: clients,
            lin_read: Arc::new(Mutex::new(LinRead::new()))
        }
    }

    pub fn merge_lin_read(&mut self, src: &LinRead) {
        let mut guard = self.lin_read.lock();

        super::merge_lin_reads(&mut guard, &src);
    }

    pub fn get_lin_read(&self) -> LinRead {
        self.lin_read.lock().clone()
    }

    pub fn alter(&self, op: Operation) -> Result<(), Error> {
        let dc = self.any_client();
        match dc.alter(&op) {
            Ok(_) => Ok(()),
            Err(e) => Err(e)
        }
    }

    pub fn any_client(&self) -> Arc<DgraphClient> {
        let between = Range::new(0, self.dc.len());
        let mut rng = rand::thread_rng();
        self.dc[between.ind_sample(&mut rng)].clone()
    }

    pub fn delete_edges(&self, mu: Mutation, uid: &str, predicates: Vec<String>) {
        let mut mu = mu;

        for pred in predicates {
            let mut object_value = Value::new();
            object_value.set_default_val("_STAR_ALL_".to_owned());
            let mut nquad = NQuad::new();
            nquad.set_subject(uid.to_string());
            nquad.set_predicate(pred);
            nquad.set_object_value(object_value);
            mu.mut_del().push(nquad)
        }
    }
}


