use crate::config::{Config, ShareConfig};
use crate::error::{Result, WatchdogError};
use crate::transcode::PresetSnapshot;
use std::path::Path;
use toml_edit::{value, Array, ArrayOfTables, DocumentMut, Item, Table};

const TEMPLATE_CONFIG: &str = include_str!("../.watchdog/watchdog.toml.example");

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EditableLibrary {
    pub name: String,
    pub local_mount: String,
    pub remote_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BasicSettings {
    pub local_mode: bool,
    pub nfs_server: String,
    pub libraries: Vec<EditableLibrary>,
    pub transcode_temp: String,
    pub default_preset: PresetSnapshot,
    pub scan_interval_seconds: u64,
    pub notify_webhook_url: String,
    pub notify_events: Vec<String>,
}

impl BasicSettings {
    pub fn from_config(config: &Config, base_dir: &Path) -> Self {
        Self {
            local_mode: config.local_mode,
            nfs_server: config.nfs.server.clone(),
            libraries: config
                .shares
                .iter()
                .map(|share| EditableLibrary {
                    name: share.name.clone(),
                    local_mount: share.local_mount.clone(),
                    remote_path: share.remote_path.clone(),
                })
                .collect(),
            transcode_temp: config.paths.transcode_temp.clone(),
            default_preset: PresetSnapshot::normalized(
                base_dir,
                &config.transcode.preset_file,
                &config.transcode.preset_name,
                &config.transcode.target_codec,
            ),
            scan_interval_seconds: config.scan.interval_seconds,
            notify_webhook_url: config.notify.webhook_url.clone(),
            notify_events: config.notify.events.clone(),
        }
    }

    pub fn to_config(&self) -> Config {
        let mut config = Config::default_config();
        self.apply_to_config(&mut config);
        config
    }

    pub fn apply_to_config(&self, config: &mut Config) {
        config.local_mode = self.local_mode;
        config.nfs.server = self.nfs_server.clone();
        config.shares = self
            .libraries
            .iter()
            .map(|library| ShareConfig {
                name: library.name.clone(),
                remote_path: library.remote_path.clone(),
                local_mount: library.local_mount.clone(),
            })
            .collect();
        config.paths.transcode_temp = self.transcode_temp.clone();
        config.transcode.preset_file = self.default_preset.preset_file.clone();
        config.transcode.preset_name = self.default_preset.preset_name.clone();
        config.transcode.target_codec = self.default_preset.target_codec.clone();
        config.scan.interval_seconds = self.scan_interval_seconds;
        config.notify.webhook_url = self.notify_webhook_url.clone();
        config.notify.events = self.notify_events.clone();
        config.normalize();
    }

    pub fn validate_with_base_dir(&self, base_dir: &Path) -> Vec<String> {
        let config = self.to_config();
        config.validate_with_base_dir(base_dir)
    }
}

fn load_doc_or_template(path: &Path) -> Result<DocumentMut> {
    if path.exists() {
        let content = std::fs::read_to_string(path)?;
        match content.parse::<DocumentMut>() {
            Ok(doc) => Ok(doc),
            Err(_) => TEMPLATE_CONFIG.parse::<DocumentMut>().map_err(|err| {
                WatchdogError::Config(format!("failed to parse embedded config template: {err}"))
            }),
        }
    } else {
        TEMPLATE_CONFIG.parse::<DocumentMut>().map_err(|err| {
            WatchdogError::Config(format!("failed to parse embedded config template: {err}"))
        })
    }
}

fn ensure_table(item: &mut Item) -> &mut Table {
    if !item.is_table() {
        *item = Item::Table(Table::new());
    }
    item.as_table_mut().expect("item converted to table")
}

fn event_array(events: &[String]) -> Array {
    let mut array = Array::default();
    for event in events {
        array.push(event.as_str());
    }
    array
}

pub fn save_basic_settings_to_path(path: &Path, settings: &BasicSettings) -> Result<()> {
    let mut doc = load_doc_or_template(path)?;

    doc["local_mode"] = value(settings.local_mode);
    {
        let nfs = ensure_table(&mut doc["nfs"]);
        nfs["server"] = value(settings.nfs_server.as_str());
    }

    let mut shares = ArrayOfTables::new();
    for library in &settings.libraries {
        let mut table = Table::new();
        table["name"] = value(library.name.as_str());
        table["remote_path"] = value(library.remote_path.as_str());
        table["local_mount"] = value(library.local_mount.as_str());
        shares.push(table);
    }
    doc["shares"] = Item::ArrayOfTables(shares);

    {
        let transcode = ensure_table(&mut doc["transcode"]);
        transcode["target_codec"] = value(settings.default_preset.target_codec.as_str());
        transcode["preset_file"] = value(settings.default_preset.preset_file.as_str());
        transcode["preset_name"] = value(settings.default_preset.preset_name.as_str());
    }

    {
        let scan = ensure_table(&mut doc["scan"]);
        scan["interval_seconds"] = value(settings.scan_interval_seconds as i64);
    }

    {
        let notify = ensure_table(&mut doc["notify"]);
        notify["webhook_url"] = value(settings.notify_webhook_url.as_str());
        notify["events"] = value(event_array(&settings.notify_events));
    }

    {
        let paths = ensure_table(&mut doc["paths"]);
        paths["transcode_temp"] = value(settings.transcode_temp.as_str());
    }

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, doc.to_string())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn sample_settings() -> BasicSettings {
        BasicSettings {
            local_mode: false,
            nfs_server: "10.0.0.20".to_string(),
            libraries: vec![
                EditableLibrary {
                    name: "movies".to_string(),
                    local_mount: "/mnt/media/Movies".to_string(),
                    remote_path: "/srv/media/Movies".to_string(),
                },
                EditableLibrary {
                    name: "shows".to_string(),
                    local_mount: "/mnt/media/TV".to_string(),
                    remote_path: "/srv/media/TV".to_string(),
                },
            ],
            transcode_temp: "/tmp/watchdog".to_string(),
            default_preset: PresetSnapshot {
                preset_file: "presets/AV1_MKV.json".to_string(),
                preset_name: "AV1_MKV".to_string(),
                target_codec: "av1".to_string(),
            },
            scan_interval_seconds: 900,
            notify_webhook_url: "https://example.com/hook".to_string(),
            notify_events: vec![
                "replacement_summary".to_string(),
                "cooldown_alert".to_string(),
            ],
        }
    }

    #[test]
    fn basic_settings_round_trip_matches_expected_config_fields() {
        let settings = sample_settings();
        let cfg = settings.to_config();
        let round_trip = BasicSettings::from_config(&cfg, Path::new("/tmp"));

        assert_eq!(round_trip.local_mode, settings.local_mode);
        assert_eq!(round_trip.nfs_server, settings.nfs_server);
        assert_eq!(round_trip.libraries, settings.libraries);
        assert_eq!(round_trip.transcode_temp, settings.transcode_temp);
        assert_eq!(round_trip.default_preset.preset_name, settings.default_preset.preset_name);
        assert_eq!(
            round_trip.default_preset.target_codec,
            settings.default_preset.target_codec
        );
        assert_eq!(round_trip.scan_interval_seconds, settings.scan_interval_seconds);
        assert_eq!(round_trip.notify_webhook_url, settings.notify_webhook_url);
        assert_eq!(round_trip.notify_events, settings.notify_events);
    }

    #[test]
    fn save_basic_settings_preserves_advanced_values_in_existing_doc() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("watchdog.toml");
        fs::write(
            &path,
            r#"
local_mode = true

[nfs]
server = ""

[[shares]]
name = "old"
remote_path = ""
local_mount = "/old"

[transcode]
target_codec = "hevc"
preset_file = "presets/HEVC_MKV.json"
preset_name = "HEVC_MKV"
timeout_seconds = 999

[scan]
interval_seconds = 300

[safety]
min_file_age_seconds = 123

[notify]
webhook_url = ""
events = ["cooldown_alert"]

[paths]
transcode_temp = "tmp"
"#,
        )
        .unwrap();

        save_basic_settings_to_path(&path, &sample_settings()).unwrap();
        let cfg = Config::load(&path).unwrap();

        assert!(!cfg.local_mode);
        assert_eq!(cfg.nfs.server, "10.0.0.20");
        assert_eq!(cfg.shares.len(), 2);
        assert_eq!(cfg.transcode.timeout_seconds, 999);
        assert_eq!(cfg.safety.min_file_age_seconds, 123);
        assert_eq!(cfg.transcode.preset_name, "AV1_MKV");
        assert_eq!(cfg.paths.transcode_temp, "/tmp/watchdog");
    }
}
