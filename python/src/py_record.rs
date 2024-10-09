use core::slice;
use std::collections::HashMap;

use pyo3::prelude::*;

use torustiq_common::
    ffi::{types::module::{Record, RecordMetadata}, utils::strings::cchar_to_string};


/// A Python representation of Record object
#[pyclass]
#[derive(Clone)]
pub struct PyRecord {
    #[pyo3(get, set)]
    content: Vec<u8>,
    #[pyo3(get, set)]
    metadata: HashMap<String, String>,
}

#[pymethods]
impl PyRecord {
    #[new]
    #[pyo3(text_signature = "(content, metadata)")]
    fn new(content: Vec<u8>, metadata: Option<HashMap<String, String>>) -> Self {
        let metadata = match metadata {
            Some(m) => m,
            None => HashMap::new(),
        };
        Self {
            content,
            metadata
        }
    }
}

impl From<Record> for PyRecord {
    fn from(value: Record) -> Self {
        let content = value.content;
        let content = unsafe { slice::from_raw_parts_mut(content.bytes, content.len) };
        let content = Vec::from(content);
        let mtd_len = value.metadata.len as usize;
        let metadata: Vec<RecordMetadata> = unsafe { Vec::from_raw_parts(value.metadata.data, mtd_len, mtd_len) };
        let metadata: HashMap<String, String> = metadata.into_iter()
            .map(|record| (cchar_to_string(record.name), cchar_to_string(record.value)))
            .collect();
        
        PyRecord {
            content,
            metadata,
        }
    }
}

impl Into<Record> for PyRecord {
    fn into(self) -> Record {
        Record::from_std_types(self.content, self.metadata)
    }
}