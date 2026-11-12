use super::*;

/// A facet image as an older release stored it: a whole database,
/// serialized into its root's `_cf_FACETS` row.
fn legacy_image() -> Vec<u8> {
    let facet = Connection::open_in_memory().unwrap();
    schema(&facet).unwrap();
    facet
        .execute_batch("CREATE TABLE notes (body TEXT); INSERT INTO notes VALUES ('kept');")
        .unwrap();
    facet
        .serialize(rusqlite::DatabaseName::Main)
        .unwrap()
        .to_vec()
}

#[test]
fn imports_legacy_image_after_replication_opened_the_facet() {
    let dir = tempfile::tempdir().unwrap();
    let root_path = dir.path().join("root.sqlite");
    let facet_file = dir.path().join("facet.sqlite");
    let facet_path = vec!["child".to_string()];

    let root = Connection::open(&root_path).unwrap();
    schema(&root).unwrap();
    root.execute(
        "INSERT INTO _cf_FACETS (scope, path, image) VALUES (?1, ?2, ?3)",
        rusqlite::params![
            "root",
            serde_json::to_string(&facet_path).unwrap(),
            legacy_image()
        ],
    )
    .unwrap();

    // Replication opens the facet first, as `open_embedded`'s callers do:
    // it creates its control tables and keeps a read transaction open.
    let _capture = celld_ltx::db::Db::open(&facet_file).unwrap();

    let mut connection = Connection::open(&facet_file).unwrap();
    let pages: u64 = connection
        .query_row("PRAGMA page_count", [], |row| row.get(0))
        .unwrap();
    assert!(
        pages > 0,
        "the file is not empty once replication opened it"
    );
    assert!(holds_only_control_tables(&connection).unwrap());
    import_legacy_facet(&mut connection, &root, "root", &facet_path).unwrap();
    schema(&connection).unwrap();

    let body: String = connection
        .query_row("SELECT body FROM notes", [], |row| row.get(0))
        .unwrap();
    assert_eq!(body, "kept");
    assert!(!holds_only_control_tables(&connection).unwrap());
}

#[test]
fn a_cleared_facet_does_not_look_new() {
    let connection = Connection::open_in_memory().unwrap();
    connection
        .execute_batch(
            "CREATE TABLE _litestream_seq (id INTEGER PRIMARY KEY, seq INTEGER);\
             CREATE TABLE _litestream_lock (id INTEGER);",
        )
        .unwrap();
    assert!(holds_only_control_tables(&connection).unwrap());
    // deleteAll keeps these celld tables and reruns `schema`.
    schema(&connection).unwrap();
    assert!(!holds_only_control_tables(&connection).unwrap());
}
