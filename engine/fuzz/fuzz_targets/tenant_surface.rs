#![no_main]

use arbitrary::Arbitrary;
use kyrodb_engine::{filter_tenant_results, TenantManager, TenantSearchResult};
use libfuzzer_sys::fuzz_target;

#[derive(Debug, Arbitrary)]
struct TenantRow {
    id: String,
    distance: f32,
}

#[derive(Debug, Arbitrary)]
struct TenantInput {
    tenant_id: String,
    namespaced_id: String,
    doc_id: u64,
    rows: Vec<TenantRow>,
}

fuzz_target!(|input: TenantInput| {
    let manager = TenantManager::new();

    let _ = manager.allocate_doc_id(&input.tenant_id);
    let _ = manager.namespace_doc_id(&input.tenant_id, input.doc_id);
    let _ = manager.parse_tenant_id(&input.namespaced_id);
    let _ = manager.parse_doc_id(&input.namespaced_id);
    let _ = manager.belongs_to_tenant(&input.namespaced_id, &input.tenant_id);

    let rows: Vec<TenantSearchResult> = input
        .rows
        .into_iter()
        .take(256)
        .map(|r| TenantSearchResult::new(r.id, r.distance))
        .collect();

    let filtered = filter_tenant_results(rows, &input.tenant_id);
    let expected_prefix = format!("{}:", input.tenant_id);
    for row in filtered {
        assert!(
            row.id.starts_with(&expected_prefix),
            "cross-tenant row leaked through filter"
        );
    }
});
