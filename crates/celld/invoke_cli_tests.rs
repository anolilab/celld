use super::*;

const SECRET: &str = "s3cret-value-1234";

fn parse(arguments: &[&str]) -> Options {
    options_from_arguments(arguments.iter().map(|argument| argument.to_string()))
        .expect("parse")
        .expect("not help")
}

fn environment(name: &str) -> Option<OsString> {
    (name == "APP_TOKEN").then(|| OsString::from(SECRET))
}

fn no_stdin() -> anyhow::Result<Vec<u8>> {
    bail!("standard input was not expected")
}

#[test]
fn a_header_env_value_never_appears_on_the_command_line() {
    let arguments = [
        "api=v2",
        "/hello",
        "--header-env",
        "authorization=APP_TOKEN",
    ];
    let options = parse(&arguments);
    // Everything parsed from argv names the variable, never its value.
    assert_eq!(
        options.header_env,
        vec![("authorization".to_string(), "APP_TOKEN".to_string())]
    );
    assert!(options.headers.is_empty());
    assert!(!format!("{options:?}").contains(SECRET));
    assert!(arguments.iter().all(|argument| !argument.contains(SECRET)));

    // The value comes from the environment when the request is built.
    let request = options.request(environment, no_stdin).expect("request");
    assert_eq!(
        request.headers,
        vec![("authorization".to_string(), SECRET.to_string())]
    );
    assert_eq!(request.script, "api");
    assert_eq!(request.version, "v2");
    assert_eq!(request.method, "GET");
    assert_eq!(request.path, "/hello");
}

#[test]
fn an_unset_variable_is_refused_without_echoing_it() {
    let options = parse(&["api=v2", "--header-env", "authorization=MISSING_VARIABLE"]);
    let error = format!(
        "{:#}",
        options
            .request(environment, no_stdin)
            .expect_err("an unset variable stops the command")
    );
    assert!(error.contains("authorization"), "{error}");
    assert!(error.contains("not set"), "{error}");
    assert!(!error.contains("MISSING_VARIABLE"), "{error}");
}

#[test]
fn a_value_typed_where_a_variable_belongs_is_not_echoed() {
    for typed in [
        "authorization=Bearer abc123",
        "authorization=abc.def-ghi",
        "authorization=",
    ] {
        let error = options_from_arguments(["api=v2", "--header-env", typed].map(String::from))
            .expect_err("not a variable name");
        let text = format!("{error:#}");
        assert!(!text.contains("abc"), "{text}");
        assert!(!text.contains("Bearer"), "{text}");
    }
    options_from_arguments(["--header-env", "no-equals-sign"].map(String::from))
        .expect_err("needs NAME=VARIABLE");
}

#[test]
fn a_value_that_cannot_be_a_header_is_refused_without_echoing_it() {
    let options = parse(&["api=v2", "--header-env", "x-key=APP_TOKEN"]);
    let error = format!(
        "{:#}",
        options
            .request(|_| Some(OsString::from("line\nbreak-secret")), no_stdin)
            .expect_err("a newline is not a header value")
    );
    assert!(!error.contains("break-secret"), "{error}");
    assert!(!error.contains("APP_TOKEN"), "{error}");
}

#[test]
fn the_method_defaults_to_post_with_a_body() {
    let options = parse(&["api=v2", "/x", "--data", "{}"]);
    let request = options.request(environment, no_stdin).unwrap();
    assert_eq!(request.method, "POST");
    assert_eq!(BASE64.decode(request.body).unwrap(), b"{}");

    let options = parse(&["api=v2", "-X", "DELETE", "/x"]);
    assert_eq!(
        options.request(environment, no_stdin).unwrap().method,
        "DELETE"
    );

    let options = parse(&["api=v2", "--data-file", "-"]);
    let request = options
        .request(environment, || Ok(b"from stdin".to_vec()))
        .unwrap();
    assert_eq!(BASE64.decode(request.body).unwrap(), b"from stdin");
}

#[test]
fn the_target_must_be_a_script_and_a_version() {
    for bad in [
        &[][..],
        &["api"][..],
        &["Api=v1"][..],
        &["api=v1", "/a", "/b"][..],
    ] {
        parse(bad)
            .request(environment, no_stdin)
            .expect_err("not a valid target");
    }
}
