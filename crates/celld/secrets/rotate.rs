//! Re-sealing every secrets object of the fleet under a new key.

use anyhow::{bail, ensure};
use zeroize::Zeroizing;

use super::{read, update, Scope, SecretSet, SecretsKey};
use crate::bucket::Bucket;

/// Walks that start over before a run gives up. A put with the old key can
/// create an object that an earlier listing did not hold, so each walk lists
/// the owners again; a run ends on the first walk that finds nothing to write.
const MAX_WALKS: usize = 4;

/// How an id found under a prefix becomes the owner it names.
type ScopeOf = fn(&str) -> Scope<'_>;

/// What a [`rotate`] re-sealed.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Rotated {
    /// Objects written: script secrets objects and store objects.
    pub objects: usize,
    /// Entries re-sealed under the new key.
    pub entries: usize,
}

/// Re-seal every entry of every script and every Secrets Store under `new`.
///
/// Each object is one conditional write through [`update`], so a `put` that
/// races it either lands first (and is re-sealed with the rest) or retries
/// against the re-sealed object. An entry already under `new` is left alone,
/// and an object that holds only such entries is not written, so a run that
/// stopped half way resumes where it stopped. The values do not change, so the
/// fleet pointer's `secrets_revisions` stays as it is and no node rebuilds.
///
/// The owners are listed again after a walk that wrote something, and the run
/// ends on a walk that wrote nothing: an object created under the old key
/// after the first listing is found and re-sealed too. If objects keep
/// appearing, the run fails and names the last one.
///
/// An entry under neither `current` nor `new` stops the run with an error that
/// names the object and the entry, never a value. Objects before it stay
/// re-sealed.
pub async fn rotate(
    bucket: &Bucket,
    current: &SecretsKey,
    new: &SecretsKey,
) -> anyhow::Result<Rotated> {
    rotate_with(bucket, current, new, async |_| {}).await
}

/// [`rotate`], running `after_walk` with the number of each walk that wrote
/// something, before the next listing.
async fn rotate_with(
    bucket: &Bucket,
    current: &SecretsKey,
    new: &SecretsKey,
    mut after_walk: impl AsyncFnMut(usize),
) -> anyhow::Result<Rotated> {
    ensure!(
        current.id != new.id,
        "the new secrets key is the current one; rotate to a different key"
    );
    let mut total = Rotated::default();
    let mut last = String::new();
    for walk_number in 1..=MAX_WALKS {
        let (walked, newest) = walk(bucket, current, new).await?;
        if walked.objects == 0 {
            return Ok(total);
        }
        total.objects += walked.objects;
        total.entries += walked.entries;
        last = newest;
        after_walk(walk_number).await;
    }
    bail!(
        "objects kept appearing under another key during {MAX_WALKS} walks, the last one {last}; \
         switch every `celld secret put` to the new key, then run rotate again"
    )
}

/// One walk over every script and every store. Returns what it wrote and the key
/// of the last object it wrote.
async fn walk(
    bucket: &Bucket,
    current: &SecretsKey,
    new: &SecretsKey,
) -> anyhow::Result<(Rotated, String)> {
    let kinds: [(&str, ScopeOf); 2] = [
        (Scope::SCRIPT_PREFIX, |id| Scope::Script(id)),
        (Scope::STORE_PREFIX, |id| Scope::Store(id)),
    ];
    let mut walked = Rotated::default();
    let mut last = String::new();
    for (prefix, scope_of) in kinds {
        let mut ids: Vec<String> = bucket
            .common_prefixes(prefix)
            .await?
            .into_iter()
            .filter_map(|child| child.strip_prefix(prefix).map(str::to_string))
            .collect();
        ids.sort();
        for id in &ids {
            let scope = scope_of(id);
            // A directory that is not a valid owner is not ours to touch.
            if scope.check().is_err() {
                continue;
            }
            let entries = rotate_in(bucket, scope, current, new).await?;
            if entries > 0 {
                walked.objects += 1;
                walked.entries += entries;
                last = scope.object_key();
            }
        }
    }
    Ok((walked, last))
}

/// Re-seal one object. Returns how many entries it re-sealed; zero when the
/// object is absent, empty, or already entirely under `new`.
async fn rotate_in(
    bucket: &Bucket,
    scope: Scope<'_>,
    current: &SecretsKey,
    new: &SecretsKey,
) -> anyhow::Result<usize> {
    let behind = |set: &SecretSet| set.secrets.values().any(|entry| entry.kid != new.id);
    match read(bucket, scope).await? {
        Some((set, _)) if behind(&set) => {}
        _ => return Ok(0),
    }
    let key = scope.object_key();
    let mut resealed = 0;
    update(bucket, scope, |set| {
        resealed = 0;
        for (name, sealed) in &mut set.secrets {
            if sealed.kid == new.id {
                continue;
            }
            anyhow::ensure!(
                sealed.kid == current.id,
                "{key}: secret {name} is sealed under key {}, which is neither the current key \
                 {} nor the new key {}; objects before it are re-sealed, so run again after \
                 fixing this one",
                sealed.kid,
                current.id,
                new.id
            );
            let value = Zeroizing::new(current.open(scope, name, sealed)?);
            *sealed = new.seal(scope, name, &value)?;
            resealed += 1;
        }
        Ok(())
    })
    .await?;
    Ok(resealed)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::super::tests::{
        bucket, key, manifest, plant, store_binding, store_manifest, values,
    };
    use super::super::{delete, load, load_stores, object_key, put, put_store, Keys, Scope};
    use super::*;

    fn manifest_of(script: &str) -> crate::protocol::Manifest {
        let mut manifest = manifest(&[]);
        manifest.script_name = script.to_string();
        manifest
    }

    /// Secrets of two scripts and a store, all under `old`.
    async fn fleet_under(bucket: &Bucket, old: &SecretsKey) {
        let none = BTreeSet::new();
        put(bucket, old, "app", "A", "one", &none).await.unwrap();
        put(bucket, old, "app", "B", "two", &none).await.unwrap();
        put(bucket, old, "other", "C", "three", &none)
            .await
            .unwrap();
        put_store(bucket, old, "st1", "k", "four").await.unwrap();
    }

    #[tokio::test]
    async fn rotate_re_seals_everything_and_the_new_key_alone_then_loads() {
        let directory = tempfile::tempdir().unwrap();
        let bucket = bucket(&directory);
        let (old, new) = (key(3), key(4));
        fleet_under(&bucket, &old).await;
        let rotated = rotate(&bucket, &old, &new).await.unwrap();
        assert_eq!(
            rotated,
            Rotated {
                objects: 3,
                entries: 4
            }
        );

        let only_new = || Ok(Some(Keys::only(&new)));
        let loaded = load(&bucket, &manifest(&[]), only_new).await.unwrap();
        assert_eq!(
            values(loaded),
            [
                ("A".to_string(), "one".to_string()),
                ("B".to_string(), "two".to_string())
            ]
        );
        let store_manifest = store_manifest(serde_json::json!([store_binding("S", "st1", "k")]));
        let stores = load_stores(&bucket, &store_manifest, &BTreeMap::new(), only_new)
            .await
            .unwrap();
        assert_eq!(stores[0].value.as_deref().unwrap(), "four");
        // The old key alone no longer opens them, and a put with the new key
        // works now that every entry is under it.
        assert!(load(&bucket, &manifest(&[]), || Ok(Some(Keys::only(&old))))
            .await
            .is_err());
        put(&bucket, &new, "app", "D", "five", &BTreeSet::new())
            .await
            .unwrap();
        let stored = String::from_utf8(
            bucket
                .get(&object_key("app"))
                .await
                .unwrap()
                .unwrap()
                .0
                .to_vec(),
        )
        .unwrap();
        assert!(!stored.contains(old.id()), "{stored}");
    }

    #[tokio::test]
    async fn rotate_is_idempotent_and_resumes_after_a_stop() {
        let directory = tempfile::tempdir().unwrap();
        let bucket = bucket(&directory);
        let (old, new, stranger) = (key(3), key(4), key(5));
        fleet_under(&bucket, &old).await;
        // `other` holds an entry that neither key opens, so the run stops there.
        plant(
            &bucket,
            Scope::Script("other"),
            &stranger,
            "BAD",
            "unknown-value",
        )
        .await;

        let error = rotate(&bucket, &old, &new).await.unwrap_err();
        let text = format!("{error:#}");
        assert!(
            text.contains("deploy/other/secrets.json") && text.contains("BAD"),
            "{text}"
        );
        assert!(!text.contains("unknown-value") && !text.contains("three"));
        // `app` sorts before `other` and was re-sealed; the store comes after.
        let (set, _) = read(&bucket, Scope::Script("app")).await.unwrap().unwrap();
        assert!(set.secrets.values().all(|entry| entry.kid == new.id()));
        let (set, _) = read(&bucket, Scope::Store("st1")).await.unwrap().unwrap();
        assert!(set.secrets.values().all(|entry| entry.kid == old.id()));

        // Fix the stuck object and run again: only what is left is written.
        delete(&bucket, "other", "BAD").await.unwrap();
        let rotated = rotate(&bucket, &old, &new).await.unwrap();
        assert_eq!(
            rotated,
            Rotated {
                objects: 2,
                entries: 2
            }
        );
        // A third run has nothing to do and writes nothing.
        let before = bucket.get(&object_key("app")).await.unwrap().unwrap().1;
        assert_eq!(
            rotate(&bucket, &old, &new).await.unwrap(),
            Rotated::default()
        );
        assert_eq!(
            bucket.get(&object_key("app")).await.unwrap().unwrap().1,
            before
        );
        assert!(rotate(&bucket, &new, &new).await.is_err());
    }

    #[tokio::test]
    async fn rotate_leaves_the_secrets_revisions_of_the_fleet_pointer_alone() {
        let directory = tempfile::tempdir().unwrap();
        let bucket = bucket(&directory);
        let (old, new) = (key(3), key(4));
        fleet_under(&bucket, &old).await;
        let pointer = serde_json::json!({
            "script_name": "app",
            "version": "v1",
            "prefix": "deploy/app/v1",
            "rollout": { "percent": 100 },
            "secrets_revisions": { "script:app": "abc", "store:st1": "def" },
        });
        bucket
            .put_cas(
                crate::fleet::CURRENT_POINTER_KEY,
                serde_json::to_vec(&pointer).unwrap(),
                None,
            )
            .await
            .unwrap()
            .unwrap();
        let before = bucket
            .get(crate::fleet::CURRENT_POINTER_KEY)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(rotate(&bucket, &old, &new).await.unwrap().entries, 4);
        let after = bucket
            .get(crate::fleet::CURRENT_POINTER_KEY)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after.0, before.0, "rotate rewrote the fleet pointer");
        assert_eq!(after.1, before.1);
    }

    #[tokio::test]
    async fn puts_racing_a_rotate_never_leave_mixed_key_ids() {
        let directory = tempfile::tempdir().unwrap();
        let bucket = bucket(&directory);
        let (old, new) = (key(3), key(4));
        let none = BTreeSet::new();
        put(&bucket, &old, "app", "A", "one", &none).await.unwrap();
        let names_put = ["B", "C", "D", "E", "F"];
        let puts = names_put
            .iter()
            .map(|name| put(&bucket, &old, "app", name, "v", &none));
        let (rotated, results) = tokio::join!(
            rotate(&bucket, &old, &new),
            futures_util::future::join_all(puts)
        );
        rotated.unwrap();
        for result in results {
            if let Err(error) = result {
                let text = format!("{error:#}");
                assert!(text.contains("secrets key differs"), "{text}");
            }
        }
        // Whichever side won each write, one key id seals the whole object.
        let (set, _) = read(&bucket, Scope::Script("app")).await.unwrap().unwrap();
        let kids: BTreeSet<_> = set
            .secrets
            .values()
            .map(|entry| entry.kid.clone())
            .collect();
        assert_eq!(kids.len(), 1, "mixed key ids: {kids:?}");
        // One more rotate leaves nothing under the old key.
        rotate(&bucket, &old, &new).await.unwrap();
        let (set, _) = read(&bucket, Scope::Script("app")).await.unwrap().unwrap();
        assert!(set.secrets.values().all(|entry| entry.kid == new.id()));
        let loaded = load(&bucket, &manifest(&[]), || Ok(Some(Keys::only(&new))))
            .await
            .unwrap();
        assert_eq!(loaded.env.len(), set.secrets.len());
    }
    #[tokio::test]
    async fn an_object_created_with_the_old_key_during_a_run_is_rotated_too() {
        let directory = tempfile::tempdir().unwrap();
        let bucket = bucket(&directory);
        let (old, new) = (key(3), key(4));
        fleet_under(&bucket, &old).await;
        // After the first walk, a put with the old key creates objects that the
        // listing did not hold: the guard on `put` has nothing to refuse there.
        let rotated = rotate_with(&bucket, &old, &new, async |pass| {
            if pass == 1 {
                let none = BTreeSet::new();
                put(&bucket, &old, "late", "L", "late-value", &none)
                    .await
                    .unwrap();
                put_store(&bucket, &old, "late-store", "k", "late-value")
                    .await
                    .unwrap();
            }
        })
        .await
        .unwrap();
        assert_eq!(
            rotated,
            Rotated {
                objects: 5,
                entries: 6
            }
        );
        let only_new = || Ok(Some(Keys::only(&new)));
        let loaded = load(&bucket, &manifest_of("late"), only_new).await.unwrap();
        assert_eq!(
            values(loaded),
            [("L".to_string(), "late-value".to_string())]
        );
        let (set, _) = read(&bucket, Scope::Store("late-store"))
            .await
            .unwrap()
            .unwrap();
        assert!(set.secrets.values().all(|entry| entry.kid == new.id()));
    }

    #[tokio::test]
    async fn a_run_that_keeps_finding_old_objects_fails_and_names_one() {
        let directory = tempfile::tempdir().unwrap();
        let bucket = bucket(&directory);
        let (old, new) = (key(3), key(4));
        let none = BTreeSet::new();
        put(&bucket, &old, "app", "A", "one", &none).await.unwrap();
        let count = std::cell::Cell::new(0);
        let error = rotate_with(&bucket, &old, &new, async |_| {
            count.set(count.get() + 1);
            let name = format!("late{}", count.get());
            put(&bucket, &old, &name, "L", "v", &none).await.unwrap();
        })
        .await
        .unwrap_err();
        let text = format!("{error:#}");
        assert!(
            text.contains("deploy/late") && text.contains("secrets.json"),
            "{text}"
        );
        assert!(!text.contains("rotated"), "{text}");
    }
}
