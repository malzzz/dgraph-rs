use std::sync::Arc;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};

use grpcio::{ChannelBuilder, EnvBuilder};
use parking_lot::{Mutex, RwLock};
use protobuf::SingularPtrField;
use rand;
use rand::distributions::{IndependentSample, Range};
use serde_json;

use grpc::api::{LinRead, Mutation, NQuad, Operation, TxnContext, Value, Value_oneof_val};
use grpc::api_grpc::DgraphClient;

use error::{Error, ErrorKind};
use transaction::Transaction;

#[derive(Debug)]
pub enum NodeId {
    UID(String),
    Blank(String)
}

#[derive(Debug, Deserialize)]
pub struct Query {
    q: Vec<HashMap<String, String>>
}

pub struct Dgraph {
    dc: Vec<Arc<DgraphClient>>,
    lin_read: Arc<Mutex<LinRead>>,
    nodes: Arc<RwLock<BTreeMap<String, NodeId>>>,
    counter: AtomicUsize,
}

impl Dgraph {
    pub fn new(clients: Vec<Arc<DgraphClient>>) -> Self {
        Dgraph {
            dc: clients,
            lin_read: Arc::new(Mutex::new(LinRead::new())),
            nodes: Arc::new(RwLock::new(BTreeMap::new())),
            counter: AtomicUsize::new(0)
        }
    }

    pub fn connect(host: &str, num_connections: u64) -> Self {
        let mut clients: Vec<Arc<DgraphClient>> = Vec::new();
        for _ in 0..num_connections {
            let env = Arc::new(EnvBuilder::new().build());
            let ch = ChannelBuilder::new(env).connect(&host);
            let dc = DgraphClient::new(ch);
            clients.push(Arc::new(dc));
        }
        Dgraph::new(clients)
    }

    pub fn merge_lin_read(&self, src: &LinRead) {
        let mut guard = self.lin_read.lock();
        super::merge_lin_reads(&mut guard, &src);
        guard.unlock_fair();
    }

    pub fn get_lin_read(&self) -> LinRead {
        self.lin_read.lock().clone()
    }

    pub fn alter(&self, op: Operation) -> Result<(), Error> {
        let dc = self.any_client();
        match dc.alter(&op) {
            Ok(_) => Ok(()),
            Err(e) => Err(ErrorKind::GrpcError(e.to_string()).into())
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

    fn get_blank(&self) -> NodeId {
        let n = self.counter.fetch_add(1, Ordering::SeqCst);
        NodeId::Blank(format!("_:blank-{}", n))
    }

    fn get_uid(&self, p: &str, o: &str, txn: &mut Transaction) -> Result<NodeId, Error> {
        let qs = format!("{{\n\tq(func: eq({}, {}), first: 1) {{\n\t\tuid\n\t\t{}\n\t}}\n}}", p, o, p);
        let q = txn.query(&qs).map(|resp| serde_json::from_slice::<Query>(&resp.json).unwrap());

        if let Ok(query) = q {
            Ok(query.q.get(0).map(|kv| {
                let k = &kv[&p.to_string()];
                let v = &kv["uid"];
                self.nodes.write().insert(k.to_string(), NodeId::UID(v.clone()));
                NodeId::UID(v.to_string())
            }).unwrap_or(self.get_blank()))
        } else {
            Err(ErrorKind::UID(format!(r#"<?> <{}> "{}""#, p, o)).into())
        }
    }

    pub fn transaction<F>(&self, mut f: F) -> Result<(), Error>
        where F: FnMut(&Transaction) -> ()
    {
        let mut tx = Transaction::new(self);
        let op = f(&tx);
        tx.commit()
    }
}


