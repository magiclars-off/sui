// Copyright (c) The Move Contributors
// SPDX-License-Identifier: Apache-2.0

//! Serde compatible types to deserialize the schematized parts of the lock file (everything in the
//! [move] table).  This module does not support serialization because of limitations in the `toml`
//! crate related to serializing types as inline tables.

use std::io::{Read, Write};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use tempfile::NamedTempFile;
use toml::value::Value;

/// Lock file version written by this version of the compiler.  Backwards compatibility is
/// guaranteed (the compiler can read lock files with older versions), forward compatibility is not
/// (the compiler will fail to read lock files at newer versions).
///
/// TODO(amnn): Set to version 1 when stabilised.
pub const VERSION: u64 = 0;

#[derive(Deserialize)]
pub struct Packages {
    #[serde(rename = "package")]
    pub packages: Option<Vec<Package>>,

    #[serde(rename = "dependencies")]
    pub root_dependencies: Option<Vec<Dependency>>,

    #[serde(rename = "dev-dependencies")]
    pub root_dev_dependencies: Option<Vec<Dependency>>,
}

#[derive(Deserialize)]
pub struct Package {
    /// The name of the package (corresponds to the name field from its source manifest).
    pub name: String,

    /// Where to find this dependency.  Schema is not described in terms of serde-compatible
    /// structs, so it is deserialized into a generic data structure.
    pub source: Value,

    pub dependencies: Option<Vec<Dependency>>,
    #[serde(rename = "dev-dependencies")]
    pub dev_dependencies: Option<Vec<Dependency>>,
}

#[derive(Deserialize)]
pub struct Dependency {
    /// The name of the dependency (corresponds to the key for the dependency in the depending
    /// package's source manifest).
    pub name: String,

    /// Mappings for named addresses to apply to the package being depended on, when referred to by
    /// the depending package.
    #[serde(rename = "addr_subst")]
    pub subst: Option<Value>,

    /// Expected hash for the source and manifest of the package being depended upon.
    pub digest: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct CompilerToolchain {
    /// The Move compiler version used to compile this package.
    #[serde(rename = "compiler-version")]
    pub compiler_version: String,
    /// The Move compiler flags used to compile this package.
    #[serde(rename = "compiler-flags")]
    pub compiler_flags: Vec<String>,
}

#[derive(Serialize, Deserialize)]
pub struct Header {
    pub version: u64,
    /// A hash of the manifest file content this lock file was generated from computed using SHA-256
    /// hashing algorithm.
    pub manifest_digest: String,
    /// A hash of all the dependencies (their lock file content) this lock file depends on, computed
    /// by first hashing all lock files using SHA-256 hashing algorithm and then combining them into
    /// a single digest using SHA-256 hasher (similarly to the package digest is computed). If there
    /// are no dependencies, it's an empty string.
    pub deps_digest: String,
}

#[derive(Serialize, Deserialize)]
struct Schema<T> {
    #[serde(rename = "move")]
    move_: T,
}

impl Packages {
    /// Read packages from the lock file, assuming the file's format matches the schema expected
    /// by this lock file, and its version is not newer than the version supported by this library.
    pub fn read(lock: &mut impl Read) -> Result<(Packages, Header)> {
        let contents = {
            let mut buf = String::new();
            lock.read_to_string(&mut buf).context("Reading lock file")?;
            buf
        };
        let Schema { move_: packages } =
            toml::de::from_str::<Schema<Packages>>(&contents).context("Deserializing packages")?;

        Ok((packages, read_header(&contents)?))
    }
}

impl CompilerToolchain {
    /// Read compiler toolchain options from the lock file.
    pub fn read(lock: &mut impl Read) -> Result<CompilerToolchain> {
        let contents = {
            let mut buf = String::new();
            lock.read_to_string(&mut buf).context("Reading lock file")?;
            buf
        };

        let Schema {
            move_: compiler_toolchain,
        } = toml::de::from_str::<Schema<CompilerToolchain>>(&contents)
            .context("Deserializing compiler toolchain options")?;
        Ok(compiler_toolchain)
    }

    pub fn write<W: Write>(
        writer: &mut W,
        compiler_version: String,
        compiler_flags: Vec<String>,
    ) -> Result<()> {
        let compiler_toolchain = toml::ser::to_string(&Schema {
            move_: CompilerToolchain {
                compiler_version,
                compiler_flags,
            },
        })?;
        write!(writer, "{}", compiler_toolchain)?;
        Ok(())
    }
}

/// Read lock file header after verifying that the version of the lock is not newer than the version
/// supported by this library.
#[allow(clippy::ptr_arg)] // Allowed to avoid interface changes.
pub fn read_header(contents: &String) -> Result<Header> {
    let Schema { move_: header } =
        toml::de::from_str::<Schema<Header>>(contents).context("Deserializing lock header")?;

    if header.version > VERSION {
        bail!(
            "Lock file format is too new, expected version {} or below, found {}",
            VERSION,
            header.version
        );
    }

    Ok(header)
}

/// Write the initial part of the lock file.
pub(crate) fn write_prologue(
    file: &mut NamedTempFile,
    manifest_digest: String,
    deps_digest: String,
) -> Result<()> {
    writeln!(
        file,
        "# @generated by Move, please check-in and do not edit manually.\n"
    )?;

    let prologue = toml::ser::to_string(&Schema {
        move_: Header {
            version: VERSION,
            manifest_digest,
            deps_digest,
        },
    })?;

    write!(file, "{}", prologue)?;

    Ok(())
}
