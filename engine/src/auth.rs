// Authentication and API key management for multi-tenant KyroDB
//
// Design:
// - Fast in-memory HashMap lookup for O(1) API key validation
// - API key format: kyro_<tenant_id>_<secret> (e.g., kyro_acme_a3f9d8e2c1b4...)
// - Support file-based key storage with hot reload
// - No crypto overhead: simple string comparison
//
// Performance: ~50ns per validation (in-memory HashMap lookup)

use anyhow::{Context, Result};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;
use std::sync::Arc;

/// API key format: kyro_<tenant_id>_<secret>
pub type ApiKey = String;

/// Tenant information associated with an API key
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct TenantInfo {
    /// Unique tenant identifier (alphanumeric, underscores)
    pub tenant_id: String,

    /// Human-readable tenant name
    pub tenant_name: String,

    /// Maximum queries per second allowed
    pub max_qps: u32,

    /// Maximum vectors this tenant can store
    pub max_vectors: usize,

    /// Whether this API key is currently enabled
    pub enabled: bool,

    /// ISO 8601 timestamp when key was created
    #[serde(default)]
    pub created_at: String,
}

impl TenantInfo {
    /// Validate tenant configuration
    pub fn validate(&self) -> Result<()> {
        anyhow::ensure!(!self.tenant_id.is_empty(), "tenant_id cannot be empty");

        anyhow::ensure!(
            self.tenant_id
                .chars()
                .all(|c| c.is_alphanumeric() || c == '_'),
            "tenant_id must be alphanumeric with underscores only: {}",
            self.tenant_id
        );

        anyhow::ensure!(!self.tenant_name.is_empty(), "tenant_name cannot be empty");

        anyhow::ensure!(
            self.max_qps > 0,
            "max_qps must be > 0, got {}",
            self.max_qps
        );

        anyhow::ensure!(
            self.max_vectors > 0,
            "max_vectors must be > 0, got {}",
            self.max_vectors
        );

        // Validate ISO 8601 timestamp format if present
        if !self.created_at.is_empty() {
            chrono::DateTime::parse_from_rfc3339(&self.created_at).with_context(|| {
                format!(
                    "created_at must be valid ISO 8601 timestamp: {}",
                    self.created_at
                )
            })?;
        }

        Ok(())
    }
}

/// API keys configuration file format
#[derive(Debug, Serialize, Deserialize)]
struct ApiKeysFile {
    api_keys: Vec<ApiKeyEntry>,
}

/// Single API key entry in configuration file
#[derive(Debug, Serialize, Deserialize)]
struct ApiKeyEntry {
    key: String,
    #[serde(flatten)]
    tenant_info: TenantInfo,
}

/// Fast in-memory API key validator
///
/// Provides O(1) lookup for API key validation with support for
/// hot reload (key rotation without restart).
pub struct AuthManager {
    /// In-memory map of API keys to tenant info
    api_keys: Arc<RwLock<HashMap<ApiKey, TenantInfo>>>,
}

impl AuthManager {
    /// Create new empty auth manager
    pub fn new() -> Self {
        Self {
            api_keys: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Load API keys from YAML file
    ///
    /// Expected format:
    /// ```yaml
    /// api_keys:
    ///   - key: kyro_acme_corp_a3f9d8e2c1b4567890abcdef12345678
    ///     tenant_id: acme_corp
    ///     tenant_name: Acme Corporation
    ///     max_qps: 1000
    ///     max_vectors: 10000000
    ///     created_at: "2025-10-01T00:00:00Z"
    ///     enabled: true
    /// ```
    ///
    /// # Errors
    /// Returns error if file cannot be read, parsed, or contains invalid data
    pub fn load_from_file(&self, path: &Path) -> Result<()> {
        let content = fs::read_to_string(path)
            .with_context(|| format!("Failed to read API keys file: {:?}", path))?;

        let config: ApiKeysFile = serde_yaml::from_str(&content)
            .with_context(|| format!("Failed to parse API keys file: {:?}", path))?;

        // Validate all entries before loading
        for entry in &config.api_keys {
            Self::validate_api_key(&entry.key)?;
            entry.tenant_info.validate()?;

            // Verify tenant_id matches key prefix
            let expected_prefix = format!("kyro_{}_", entry.tenant_info.tenant_id);
            anyhow::ensure!(
                entry.key.starts_with(&expected_prefix),
                "API key {} does not match tenant_id {} (expected prefix: {})",
                entry.key,
                entry.tenant_info.tenant_id,
                expected_prefix
            );
        }

        // Atomic replacement of all keys
        let mut keys = HashMap::new();
        for entry in config.api_keys {
            keys.insert(entry.key, entry.tenant_info);
        }

        *self.api_keys.write() = keys;

        Ok(())
    }

    /// Validate API key format: kyro_<tenant_id>_<secret>
    ///
    /// Since tenant_id can contain underscores, we need special parsing logic:
    /// The secret is always the last 32+ alphanumeric characters after the final underscore.
    fn validate_api_key(key: &str) -> Result<()> {
        anyhow::ensure!(
            key.starts_with("kyro_"),
            "API key must start with 'kyro_': {}",
            key
        );

        anyhow::ensure!(
            key.len() >= 39, // "kyro_" (5) + at least 1 char tenant + "_" (1) + 32 char secret = 39 min
            "API key too short (minimum 39 characters): {}",
            key
        );

        // Find the last underscore (separates tenant_id from secret)
        let last_underscore_pos = key.rfind('_').ok_or_else(|| {
            anyhow::anyhow!(
                "API key must have format kyro_<tenant_id>_<secret>: {}",
                key
            )
        })?;

        anyhow::ensure!(
            last_underscore_pos > 5, // Must be after "kyro_"
            "API key must have format kyro_<tenant_id>_<secret>: {}",
            key
        );

        let tenant_id = &key[5..last_underscore_pos]; // After "kyro_" and before last "_"
        let secret = &key[last_underscore_pos + 1..]; // After last "_"

        anyhow::ensure!(
            !tenant_id.is_empty(),
            "tenant_id cannot be empty in API key: {}",
            key
        );

        anyhow::ensure!(
            !secret.is_empty(),
            "secret cannot be empty in API key: {}",
            key
        );

        anyhow::ensure!(
            secret.len() >= 32,
            "secret must be at least 32 characters (got {}): {}",
            secret.len(),
            key
        );

        anyhow::ensure!(
            secret.chars().all(|c| c.is_ascii_alphanumeric()),
            "secret must be alphanumeric: {}",
            key
        );

        Ok(())
    }

    /// Return all enabled tenants (deduplicated by tenant_id at caller if needed).
    pub fn enabled_tenants(&self) -> Vec<TenantInfo> {
        self.api_keys
            .read()
            .values()
            .filter(|tenant_info| tenant_info.enabled)
            .cloned()
            .collect()
    }

    /// Validate API key and return tenant info
    ///
    /// Returns None if:
    /// - API key does not exist
    /// - API key is disabled (enabled = false)
    ///
    /// # Performance
    /// O(1) lookup via HashMap. ~50ns on modern hardware.
    pub fn validate(&self, api_key: &str) -> Option<TenantInfo> {
        let keys = self.api_keys.read();
        keys.get(api_key)
            .filter(|tenant_info| tenant_info.enabled)
            .cloned()
    }

    /// Add or update a single API key (for testing/dynamic provisioning)
    pub fn add_key(&self, key: String, tenant_info: TenantInfo) -> Result<()> {
        Self::validate_api_key(&key)?;
        tenant_info.validate()?;

        // Verify tenant_id matches key prefix
        let expected_prefix = format!("kyro_{}_", tenant_info.tenant_id);
        anyhow::ensure!(
            key.starts_with(&expected_prefix),
            "API key {} does not match tenant_id {} (expected prefix: {})",
            key,
            tenant_info.tenant_id,
            expected_prefix
        );

        self.api_keys.write().insert(key, tenant_info);
        Ok(())
    }

    /// Remove an API key (for testing/key revocation)
    pub fn remove_key(&self, key: &str) -> Option<TenantInfo> {
        self.api_keys.write().remove(key)
    }

    /// Get all registered tenant IDs (for admin operations)
    ///
    /// Returns unique tenant IDs (deduplicated if multiple keys per tenant).
    pub fn list_tenants(&self) -> Vec<String> {
        self.api_keys
            .read()
            .values()
            .map(|info| info.tenant_id.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect()
    }

    /// Count of registered API keys
    pub fn key_count(&self) -> usize {
        self.api_keys.read().len()
    }

    /// Clear all API keys (for testing)
    #[cfg(test)]
    pub fn clear(&self) {
        self.api_keys.write().clear();
    }
}

impl Default for AuthManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Seek, Write};
    use tempfile::NamedTempFile;

    fn create_test_tenant_info(tenant_id: &str) -> TenantInfo {
        TenantInfo {
            tenant_id: tenant_id.to_string(),
            tenant_name: format!("{} Corp", tenant_id),
            max_qps: 1000,
            max_vectors: 1_000_000,
            enabled: true,
            created_at: "2025-10-16T00:00:00Z".to_string(),
        }
    }

    fn create_test_api_key(tenant_id: &str) -> String {
        format!("kyro_{}_a3f9d8e2c1b4567890abcdef12345678", tenant_id)
    }

    #[test]
    fn test_api_key_validation() {
        // Valid key
        assert!(
            AuthManager::validate_api_key("kyro_acme_a3f9d8e2c1b4567890abcdef12345678").is_ok()
        );

        // Invalid: missing prefix
        assert!(AuthManager::validate_api_key("acme_a3f9d8e2c1b4567890abcdef12345678").is_err());

        // Invalid: empty tenant_id
        assert!(AuthManager::validate_api_key("kyro__a3f9d8e2c1b4567890abcdef12345678").is_err());

        // Invalid: empty secret
        assert!(AuthManager::validate_api_key("kyro_acme_").is_err());

        // Invalid: secret too short
        assert!(AuthManager::validate_api_key("kyro_acme_short").is_err());

        // Invalid: non-alphanumeric secret
        assert!(
            AuthManager::validate_api_key("kyro_acme_a3f9d8e2c1b4567890abcdef123456!!").is_err()
        );
    }

    #[test]
    fn test_tenant_info_validation() {
        let valid = create_test_tenant_info("acme");
        assert!(valid.validate().is_ok());

        // Invalid: empty tenant_id
        let mut invalid = valid.clone();
        invalid.tenant_id = "".to_string();
        assert!(invalid.validate().is_err());

        // Invalid: non-alphanumeric tenant_id
        let mut invalid = valid.clone();
        invalid.tenant_id = "acme-corp".to_string();
        assert!(invalid.validate().is_err());

        // Invalid: empty tenant_name
        let mut invalid = valid.clone();
        invalid.tenant_name = "".to_string();
        assert!(invalid.validate().is_err());

        // Invalid: zero max_qps
        let mut invalid = valid.clone();
        invalid.max_qps = 0;
        assert!(invalid.validate().is_err());

        // Invalid: zero max_vectors
        let mut invalid = valid.clone();
        invalid.max_vectors = 0;
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn test_add_and_validate_key() {
        let auth = AuthManager::new();

        let key = create_test_api_key("acme");
        let tenant_info = create_test_tenant_info("acme");

        // Add key
        assert!(auth.add_key(key.clone(), tenant_info.clone()).is_ok());

        // Validate key
        let result = auth.validate(&key);
        assert!(result.is_some());
        assert_eq!(result.unwrap().tenant_id, "acme");

        // Invalid key returns None
        assert!(auth
            .validate("kyro_invalid_a3f9d8e2c1b4567890abcdef12345678")
            .is_none());
    }

    #[test]
    fn test_disabled_key_rejected() {
        let auth = AuthManager::new();

        let key = create_test_api_key("acme");
        let mut tenant_info = create_test_tenant_info("acme");
        tenant_info.enabled = false;

        auth.add_key(key.clone(), tenant_info).unwrap();

        // Disabled key returns None
        assert!(auth.validate(&key).is_none());
    }

    #[test]
    fn test_remove_key() {
        let auth = AuthManager::new();

        let key = create_test_api_key("acme");
        let tenant_info = create_test_tenant_info("acme");

        auth.add_key(key.clone(), tenant_info.clone()).unwrap();
        assert!(auth.validate(&key).is_some());

        // Remove key
        let removed = auth.remove_key(&key);
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().tenant_id, "acme");

        // Key no longer valid
        assert!(auth.validate(&key).is_none());
    }

    #[test]
    fn test_list_tenants() {
        let auth = AuthManager::new();

        auth.add_key(create_test_api_key("acme"), create_test_tenant_info("acme"))
            .unwrap();
        auth.add_key(
            create_test_api_key("startup"),
            create_test_tenant_info("startup"),
        )
        .unwrap();

        let tenants = auth.list_tenants();
        assert_eq!(tenants.len(), 2);
        assert!(tenants.contains(&"acme".to_string()));
        assert!(tenants.contains(&"startup".to_string()));
    }

    #[test]
    fn test_load_from_file() {
        let yaml_content = r#"
api_keys:
  - key: kyro_acme_corp_a3f9d8e2c1b4567890abcdef12345678
    tenant_id: acme_corp
    tenant_name: Acme Corporation
    max_qps: 1000
    max_vectors: 10000000
    enabled: true
    created_at: "2025-10-01T00:00:00Z"
  - key: kyro_startup_x_b4e8f1a9d2c3567890fedcba98765432
    tenant_id: startup_x
    tenant_name: Startup X
    max_qps: 100
    max_vectors: 100000
    enabled: true
    created_at: "2025-10-15T00:00:00Z"
"#;

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(yaml_content.as_bytes()).unwrap();
        temp_file.flush().unwrap();

        let auth = AuthManager::new();
        let result = auth.load_from_file(temp_file.path());
        if let Err(e) = &result {
            eprintln!("Load error: {:?}", e);
        }
        assert!(result.is_ok());

        assert_eq!(auth.key_count(), 2);

        // Validate both keys
        let acme = auth.validate("kyro_acme_corp_a3f9d8e2c1b4567890abcdef12345678");
        assert!(acme.is_some());
        assert_eq!(acme.unwrap().tenant_id, "acme_corp");

        let startup = auth.validate("kyro_startup_x_b4e8f1a9d2c3567890fedcba98765432");
        assert!(startup.is_some());
        assert_eq!(startup.unwrap().tenant_id, "startup_x");
    }

    #[test]
    fn test_load_invalid_file() {
        let auth = AuthManager::new();

        // Non-existent file
        assert!(auth
            .load_from_file(Path::new("/nonexistent/path.yaml"))
            .is_err());

        // Invalid YAML
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(b"invalid: yaml: content: [").unwrap();
        temp_file.flush().unwrap();
        assert!(auth.load_from_file(temp_file.path()).is_err());
    }

    #[test]
    fn test_tenant_id_mismatch_rejected() {
        let auth = AuthManager::new();

        // Key says "acme" but tenant_info says "wrong"
        let key = create_test_api_key("acme");
        let mut tenant_info = create_test_tenant_info("acme");
        tenant_info.tenant_id = "wrong".to_string();

        assert!(auth.add_key(key, tenant_info).is_err());
    }

    #[test]
    fn test_atomic_reload() {
        let yaml_v1 = r#"
api_keys:
  - key: kyro_acme_a3f9d8e2c1b4567890abcdef12345678
    tenant_id: acme
    tenant_name: Acme Corp
    max_qps: 1000
    max_vectors: 1000000
    enabled: true
"#;

        let yaml_v2 = r#"
api_keys:
  - key: kyro_startup_b4e8f1a9d2c3567890fedcba98765432
    tenant_id: startup
    tenant_name: Startup X
    max_qps: 100
    max_vectors: 100000
    enabled: true
"#;

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(yaml_v1.as_bytes()).unwrap();
        temp_file.flush().unwrap();

        let auth = AuthManager::new();
        auth.load_from_file(temp_file.path()).unwrap();

        assert_eq!(auth.key_count(), 1);
        assert!(auth
            .validate("kyro_acme_a3f9d8e2c1b4567890abcdef12345678")
            .is_some());

        // Reload with different keys
        temp_file
            .as_file_mut()
            .seek(std::io::SeekFrom::Start(0))
            .unwrap();
        temp_file.as_file_mut().set_len(0).unwrap();
        temp_file.write_all(yaml_v2.as_bytes()).unwrap();
        temp_file.flush().unwrap();

        auth.load_from_file(temp_file.path()).unwrap();

        // Old key no longer valid
        assert!(auth
            .validate("kyro_acme_a3f9d8e2c1b4567890abcdef12345678")
            .is_none());

        // New key is valid
        assert!(auth
            .validate("kyro_startup_b4e8f1a9d2c3567890fedcba98765432")
            .is_some());
        assert_eq!(auth.key_count(), 1);
    }
}
