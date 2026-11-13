//! End-to-end tests: fixture Workers served by the real `celld dev` binary.

mod e2e {
    pub mod alarms;
    pub mod body_clone;
    pub mod container_exec;
    pub mod container_images;
    pub mod d1_batch;
    pub mod d1_export;
    pub mod do_alarms;
    pub mod do_gates;
    pub mod do_ids;
    pub mod do_sql;
    pub mod do_storage;
    pub mod egress;
    pub mod facets;
    pub mod fake_engine;
    pub mod harness;
    pub mod isolates;
    pub mod node_crypto;
    pub mod operator_branches;
    pub mod operator_token;
    pub mod ownership;
    pub mod runtime_pool;
    pub mod version;
    pub mod webcrypto;
}
