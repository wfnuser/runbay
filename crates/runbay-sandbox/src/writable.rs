use anyhow::{ensure, Context, Result};
use std::collections::BTreeMap;
use std::fs;
use std::io::ErrorKind;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

/// A writable bind can update every name of an inode. Check the daemon-owned run
/// directories before launch. Links contained entirely within writable scopes are valid.
pub(super) fn check_links(workspace: &Path, extra: &[PathBuf]) -> Result<()> {
    let mut roots = std::iter::once(workspace)
        .chain(extra.iter().map(PathBuf::as_path))
        .map(fs::canonicalize)
        .collect::<std::io::Result<Vec<_>>>()?;
    roots.sort();
    let mut links = BTreeMap::new();
    let mut last_root: Option<&Path> = None;
    for root in &roots {
        if last_root.is_some_and(|parent| root.starts_with(parent)) {
            continue;
        }
        last_root = Some(root);
        for entry in WalkDir::new(root).follow_links(false) {
            let entry = entry.context("inspect writable sandbox directory")?;
            if !entry.file_type().is_file() {
                continue;
            }
            let metadata = match fs::symlink_metadata(entry.path()) {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
            if metadata.is_file() && metadata.nlink() > 1 {
                let (expected, seen, _) = links
                    .entry((metadata.dev(), metadata.ino()))
                    .or_insert_with(|| (metadata.nlink(), 0u64, entry.path().to_owned()));
                ensure!(
                    *expected == metadata.nlink(),
                    "hard link count changed during sandbox setup"
                );
                *seen += 1;
            }
        }
    }
    for (expected, seen, path) in links.into_values() {
        ensure!(
            seen == expected,
            "writable sandbox file has a hard link outside its scope: {}",
            path.display()
        );
    }
    Ok(())
}
