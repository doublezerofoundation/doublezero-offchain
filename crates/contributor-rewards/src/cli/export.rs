use super::{Exportable, OutputFormat};
use anyhow::Result;
use std::fs::{File, create_dir_all};
use std::io::Write;
use std::path::Path;
use tracing::info;

/// Options for exporting data
pub struct ExportOptions {
    pub format: OutputFormat,
    pub output_dir: Option<String>,
    pub output_file: Option<String>,
}

impl ExportOptions {
    /// Write exportable data to file or stdout
    pub fn write<T: Exportable>(&self, data: &T, default_filename: &str) -> Result<()> {
        let content = data.export(self.format)?;

        if let Some(ref file_path) = self.output_file {
            // Write to specific file
            let path = Path::new(file_path);
            if let Some(parent) = path.parent() {
                create_dir_all(parent)?;
            }
            let mut file = File::create(path)?;
            file.write_all(content.as_bytes())?;
            info!("Exported to: {}", path.display());
        } else if let Some(ref dir) = self.output_dir {
            // Write to directory with default filename
            let dir_path = Path::new(dir);
            create_dir_all(dir_path)?;

            let extension = match self.format {
                OutputFormat::Csv => "csv",
                OutputFormat::Json | OutputFormat::JsonPretty => "json",
            };

            let filename = format!("{default_filename}.{extension}");
            let file_path = dir_path.join(filename);

            let mut file = File::create(&file_path)?;
            file.write_all(content.as_bytes())?;
            info!("Exported to: {}", file_path.display());
        } else {
            // Write to stdout
            println!("{content}");
        }

        Ok(())
    }
}

/// Helper function to convert data to CSV format
pub fn to_csv_string<T: serde::Serialize>(records: &[T]) -> Result<String> {
    let mut wtr = csv::Writer::from_writer(vec![]);
    for record in records {
        wtr.serialize(record)?;
    }
    let data = wtr.into_inner()?;
    Ok(String::from_utf8(data)?)
}

/// Helper function to convert data to JSON format
pub fn to_json_string<T: serde::Serialize>(data: &T, pretty: bool) -> Result<String> {
    if pretty {
        Ok(serde_json::to_string_pretty(data)?)
    } else {
        Ok(serde_json::to_string(data)?)
    }
}
