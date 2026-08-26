use anyhow::{Context as _, Result, ensure};
use collections::IndexMap;
use gpui::SharedString;

/// A validated command template associated with an agent harness.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResumeCommandTemplate {
    pub harness: SharedString,
    pub template: SharedString,
}

/// Validates the opaque locator before it is interpolated into a shell command.
pub fn validate_resume_locator(locator: &str) -> Result<()> {
    ensure!(!locator.is_empty(), "resume locator must not be empty");
    ensure!(
        locator.len() <= 512,
        "resume locator must be at most 512 bytes"
    );
    ensure!(
        !locator.starts_with('-'),
        "resume locator must not start with '-'"
    );
    ensure!(
        locator.chars().all(|character| !character.is_control()),
        "resume locator must not contain control characters"
    );
    Ok(())
}

fn validate_resume_template(template: &str) -> Result<()> {
    ensure!(!template.is_empty(), "resume command template must not be empty");
    ensure!(
        template.chars().all(|character| !character.is_control()),
        "resume command template must not contain control characters"
    );
    ensure!(
        template.contains("{locator}"),
        "resume command template must contain '{{locator}}'"
    );
    Ok(())
}

/// Builds a command from the configured template without executing it.
pub fn build_resume_command(
    harness: &str,
    locator: &str,
    templates: &IndexMap<String, String>,
) -> Result<String> {
    validate_resume_locator(locator)?;
    let template = templates
        .get(harness)
        .with_context(|| format!("no resume command template configured for harness {harness:?}"))?;
    validate_resume_template(template)?;
    Ok(template.replace("{locator}", locator))
}

/// Formats a generated command as one non-executable shell comment line.
pub fn resume_comment(command: &str) -> Result<String> {
    ensure!(!command.is_empty(), "resume command must not be empty");
    ensure!(
        command.chars().all(|character| !character.is_control()),
        "resume command must not contain control characters"
    );
    Ok(format!("# {command}\r"))
}


#[cfg(test)]
fn defaults() -> IndexMap<String, String> {
    let mut templates = IndexMap::default();
    templates.insert(
        "omp".to_string(),
        "omp --resume {locator}".to_string(),
    );
    templates
}

#[test]
fn accepts_safe_resume_locator() {
    assert!(validate_resume_locator("session-1").is_ok());
}

#[test]
fn rejects_unsafe_resume_locators() {
    assert!(validate_resume_locator("").is_err());
    assert!(validate_resume_locator("--bad").is_err());
    assert!(validate_resume_locator("session\nsecond").is_err());
    assert!(validate_resume_locator("session\rsecond").is_err());
    assert!(validate_resume_locator("session\0").is_err());
    assert!(validate_resume_locator(&"x".repeat(513)).is_err());
    assert!(validate_resume_locator(&"é".repeat(257)).is_err());
    assert!(validate_resume_locator(&"é".repeat(256)).is_ok());
}

#[test]
fn builds_resume_command_from_harness_template() {
    assert_eq!(
        build_resume_command("omp", "session-1", &defaults()).unwrap(),
        "omp --resume session-1"
    );
}

#[test]
fn rejects_missing_harness_and_empty_locator_without_rendering() {
    assert!(build_resume_command("missing", "session-1", &defaults()).is_err());
    assert!(build_resume_command("omp", "", &defaults()).is_err());
}

#[test]
fn rejects_invalid_resume_templates() {
    let mut templates = defaults();
    templates.insert("empty".to_string(), String::new());
    assert!(build_resume_command("empty", "session-1", &templates).is_err());

    templates.insert("missing-placeholder".to_string(), "omp --resume".to_string());
    assert!(build_resume_command("missing-placeholder", "session-1", &templates).is_err());

    templates.insert(
        "control".to_string(),
        "omp --resume {locator}\n".to_string(),
    );
    assert!(build_resume_command("control", "session-1", &templates).is_err());
}

#[test]
fn renders_resume_command_as_a_single_shell_comment() {
    assert_eq!(
        resume_comment("omp --resume session-1").unwrap(),
        "# omp --resume session-1\r"
    );
    assert!(resume_comment("omp --resume session-1\n").is_err());
    assert!(resume_comment("").is_err());
}
