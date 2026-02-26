#![no_main]

use arbitrary::Arbitrary;
use kyrodb_engine::{AuthManager, TenantInfo};
use libfuzzer_sys::fuzz_target;

#[derive(Debug, Arbitrary)]
struct AuthInput {
    key: String,
    tenant_id: String,
    tenant_name: String,
    max_qps: u32,
    max_vectors: u16,
    is_admin: bool,
    enabled: bool,
}

fuzz_target!(|input: AuthInput| {
    let auth = AuthManager::new();

    let info = TenantInfo {
        tenant_id: input.tenant_id,
        tenant_name: input.tenant_name,
        max_qps: input.max_qps,
        max_vectors: usize::from(input.max_vectors),
        is_admin: input.is_admin,
        enabled: input.enabled,
        created_at: String::new(),
    };

    let add_result = auth.add_key(input.key.clone(), info.clone());
    if add_result.is_ok() {
        let maybe = auth.validate(&input.key);
        if info.enabled {
            assert!(
                maybe.is_some(),
                "enabled key added successfully must validate"
            );
        } else {
            assert!(
                maybe.is_none(),
                "disabled key added successfully must not validate"
            );
        }

        let _ = auth.list_tenants();
        let _ = auth.enabled_tenants();
        let _ = auth.remove_key(&input.key);
    }
});
