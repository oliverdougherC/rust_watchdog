use std::path::PathBuf;
use watchdog::probe::{verify_transcode, FfprobeProber};
use watchdog::traits::Transcoder;
use watchdog::transcode::HandBrakeTranscoder;

#[test]
fn real_tools_transcode_smoke() {
    if std::env::var("WATCHDOG_REAL_TOOLS").ok().as_deref() != Some("1") {
        return;
    }
    if which::which("ffprobe").is_err() || which::which("HandBrakeCLI").is_err() {
        return;
    }

    let input = match std::env::var("WATCHDOG_REAL_INPUT") {
        Ok(v) => PathBuf::from(v),
        Err(_) => return,
    };
    if !input.exists() {
        return;
    }

    let preset_file = std::env::var("WATCHDOG_REAL_PRESET_FILE")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("AV1_MKV.json"));
    let preset_name =
        std::env::var("WATCHDOG_REAL_PRESET_NAME").unwrap_or_else(|_| "AV1_MKV".to_string());
    if !preset_file.exists() {
        return;
    }

    let temp_dir = tempfile::tempdir().unwrap();
    let output = temp_dir.path().join("real_tools_output.mkv");
    let (tx, _rx) = tokio::sync::mpsc::channel(32);

    let transcoder = HandBrakeTranscoder;
    let result = transcoder
        .transcode(
            &input,
            &output,
            &preset_file,
            &preset_name,
            300,
            tx,
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        )
        .unwrap();
    assert!(result.success);
    assert!(result.output_exists);

    let prober = FfprobeProber;
    let verified = verify_transcode(&prober, &input, &output).unwrap();
    assert!(verified);
}
