//! End-to-end tests: fixture Workers served by the real `celld dev` binary.

mod e2e {
    pub mod alarms;
    pub mod d1_batch;
    pub mod do_alarms;
    pub mod do_gates;
    pub mod do_ids;
    pub mod do_sql;
    pub mod do_storage;
    pub mod facets;
    pub mod harness;
    pub mod isolates;
    pub mod node_crypto;
    pub mod webcrypto;
}
