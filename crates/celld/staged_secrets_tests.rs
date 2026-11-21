use super::*;

#[tokio::test]
async fn activation_refuses_a_version_whose_binding_a_secret_now_uses() {
    let fleet = fleet();
    deploy(&fleet, "back", "v1", &[], true).await;
    deploy(&fleet, "front", "v1", &[("API_TOKEN", "back")], true).await;
    deploy(&fleet, "front", "v2", &[], true).await;
    // The secret name is free in the deployment that serves, so `put` accepts it.
    put_secret(&fleet, "front", "API_TOKEN").await;
    let (before, token_before) = fleet_pointer(&fleet).await;

    // Rolling back to the version that binds the name would shadow the secret.
    let error = activate(&fleet.bucket, &[target("front", "v1")], None)
        .await
        .expect_err("the rollback clashes with the secret");
    assert!(
        format!("{error:#}").contains("secret API_TOKEN"),
        "{error:#}"
    );
    let (after, token_after) = fleet_pointer(&fleet).await;
    assert_eq!(token_before, token_after, "the fleet pointer moved");
    assert_eq!(before.version, after.version);
    assert_eq!(named_pointer(&fleet, "front").await.version, "v2");
}

/// `built` for a config whose `secrets.required` lists `names`.
fn built_requiring(script: &str, version: &str, names: &[&str]) -> Built {
    let mut built = built(script, version, &[]);
    built.manifest.raw_metadata["secrets"] = serde_json::json!({ "required": names });
    built
        .manifest
        .required_features
        .push(crate::protocol::FEATURE_SECRETS_REQUIRED_V1.to_string());
    built
}

async fn put_secret(fleet: &Fleet, script: &str, name: &str) {
    let key = crate::secrets::SecretsKey::parse(&base64_key()).unwrap();
    crate::secrets::put(
        &fleet.bucket,
        &key,
        script,
        name,
        "value",
        &std::collections::BTreeSet::new(),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn a_deploy_is_refused_until_every_required_secret_is_put() {
    let fleet = fleet();
    let version = || built_requiring("api", "v1", &["API_KEY", "DB_PASSWORD"]);
    let error = crate::deploy::write(&fleet.bucket, &version(), Selection::Primary)
        .await
        .expect_err("the secrets are missing");
    let text = format!("{error:#}");
    assert!(text.contains("API_KEY, DB_PASSWORD"), "{text}");
    assert!(
        crate::deploy::read_fleet_pointer(&fleet.bucket)
            .await
            .unwrap()
            .is_none(),
        "a refused deploy leaves no pointer"
    );

    put_secret(&fleet, "api", "API_KEY").await;
    let error = crate::deploy::write(&fleet.bucket, &version(), Selection::Primary)
        .await
        .expect_err("one is still missing");
    assert!(format!("{error:#}").contains("not set: DB_PASSWORD"));

    put_secret(&fleet, "api", "DB_PASSWORD").await;
    crate::deploy::write(&fleet.bucket, &version(), Selection::Primary)
        .await
        .expect("accepted once every secret is put");
}

#[tokio::test]
async fn an_activation_is_refused_while_a_required_secret_is_missing() {
    let fleet = fleet();
    // A version cannot be staged without its secrets, so one goes missing
    // between the upload and the activation.
    put_secret(&fleet, "api", "API_KEY").await;
    crate::deploy::write(
        &fleet.bucket,
        &built_requiring("api", "v1", &["API_KEY"]),
        Selection::Stage,
    )
    .await
    .expect("stage");
    crate::secrets::delete(&fleet.bucket, "api", "API_KEY", &Default::default())
        .await
        .unwrap();

    let error = activate(&fleet.bucket, &[target("api", "v1")], None)
        .await
        .expect_err("the secret is gone");
    assert!(
        format!("{error:#}").contains("not set: API_KEY"),
        "{error:#}"
    );
    assert!(crate::deploy::read_fleet_pointer(&fleet.bucket)
        .await
        .unwrap()
        .is_none());

    put_secret(&fleet, "api", "API_KEY").await;
    activate(&fleet.bucket, &[target("api", "v1")], None)
        .await
        .expect("activated after the put");
}

#[tokio::test]
async fn a_secret_the_serving_deployment_requires_cannot_be_deleted() {
    let fleet = fleet();
    put_secret(&fleet, "api", "API_KEY").await;
    put_secret(&fleet, "api", "OTHER").await;
    crate::deploy::write(
        &fleet.bucket,
        &built_requiring("api", "v1", &["API_KEY"]),
        Selection::Primary,
    )
    .await
    .expect("deploy");

    let delete = |name: &'static str| {
        let bucket = &fleet.bucket;
        async move {
            let required = crate::secret_cli::current_required(bucket, "api").await?;
            crate::secrets::delete(bucket, "api", name, &required).await
        }
    };
    let error = delete("API_KEY")
        .await
        .expect_err("the deployment requires it");
    assert!(
        format!("{error:#}").contains("secret API_KEY is required"),
        "{error:#}"
    );
    assert_eq!(
        crate::secrets::names(&fleet.bucket, "api").await.unwrap(),
        ["API_KEY", "OTHER"]
    );
    delete("OTHER")
        .await
        .expect("an unlisted secret is deleted");
}

fn base64_key() -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode([5u8; 32])
}
