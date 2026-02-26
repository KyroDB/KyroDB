#![no_main]

use kyrodb_engine::api_validation::{validate_insert_request, validate_search_request};
use kyrodb_engine::proto::{InsertRequest, SearchRequest};
use libfuzzer_sys::fuzz_target;
use prost::Message;

fuzz_target!(|data: &[u8]| {
    if let Ok(search) = SearchRequest::decode(data) {
        let _ = validate_search_request(&search);
    }

    if let Ok(insert) = InsertRequest::decode(data) {
        let _ = validate_insert_request(&insert);
    }
});
