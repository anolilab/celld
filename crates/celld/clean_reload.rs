// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The clean-reload certificate a node leaves for its successor.

use crate::asyncrt;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::path::Path;

const CLEAN_RELOAD_MARKER: &str = ".clean-reload.json";

/// The session tags of the cells a preserved restart hands on: the tags that
/// the records of the captured cells carry, newest first, and the tag of each
/// captured cell's record. A process that runs with `CELLD_LAZY_BIRTH=off`
/// writes none, so its certificate keeps the bytes of an older binary.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReloadSession {
    pub tags: Vec<String>,
    pub cells: BTreeMap<String, String>,
    /// The epoch each captured cell was resumed at.
    #[serde(default)]
    pub epochs: BTreeMap<String, u64>,
}

#[derive(Serialize)]
struct CleanReloadMarker<'a> {
    node: &'a str,
    generation: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    session: Option<&'a ReloadSession>,
}

#[derive(Deserialize)]
struct OwnedCleanReloadMarker {
    node: String,
    generation: String,
    #[serde(default)]
    session: Option<ReloadSession>,
}

/// What a clean local shutdown left for this start.
pub struct CleanReload {
    pub generation: String,
    pub session: Option<ReloadSession>,
}

/// Read the fixed-size certificate left by a clean local shutdown. The node
/// lease state machine still decides whether the named generation is live and
/// can be replaced; this local value grants no authority by itself.
pub fn take_clean_reload(data_dir: &Path, node: &str) -> Option<CleanReload> {
    let path = data_dir.join(CLEAN_RELOAD_MARKER);
    let filesystem = asyncrt::fs();
    let bytes = filesystem.read(&path).ok()?;
    let _ = filesystem.remove_file(&path);
    let marker: OwnedCleanReloadMarker = serde_json::from_slice(&bytes).ok()?;
    (marker.node == node).then_some(CleanReload {
        generation: marker.generation,
        session: marker.session,
    })
}

pub fn write_clean_reload_marker(
    data_dir: &Path,
    node: &str,
    generation: &str,
    session: Option<&ReloadSession>,
) -> anyhow::Result<()> {
    let filesystem = asyncrt::fs();
    filesystem.create_dir_all(data_dir)?;
    let marker = data_dir.join(CLEAN_RELOAD_MARKER);
    let temporary = data_dir.join(".clean-reload.tmp");
    let body = serde_json::to_vec(&CleanReloadMarker {
        node,
        generation,
        session,
    })?;
    filesystem.write(&temporary, &body)?;
    filesystem.rename(&temporary, &marker)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_marker_without_a_session_keeps_the_bytes_of_an_older_binary() {
        let body = serde_json::to_vec(&CleanReloadMarker {
            node: "n",
            generation: "g",
            session: None,
        })
        .unwrap();
        assert_eq!(body, br#"{"node":"n","generation":"g"}"#);
        let read: OwnedCleanReloadMarker = serde_json::from_slice(&body).unwrap();
        assert!(read.session.is_none());
    }

    #[test]
    fn a_marker_lists_the_tags_and_the_tag_of_each_cell() {
        let session = ReloadSession {
            tags: vec!["b".into(), "a".into()],
            cells: BTreeMap::from([("c1".into(), "a".into())]),
            epochs: BTreeMap::from([("c1".into(), 3)]),
        };
        let body = serde_json::to_vec(&CleanReloadMarker {
            node: "n",
            generation: "g",
            session: Some(&session),
        })
        .unwrap();
        let read: OwnedCleanReloadMarker = serde_json::from_slice(&body).unwrap();
        assert_eq!(read.session, Some(session));
    }
}
