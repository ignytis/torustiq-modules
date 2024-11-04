use core::slice;
use std::collections::HashMap;

use pyo3::prelude::*;

use torustiq_common::ffi::types::module::Record;


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
    #[pyo3(signature = (content, metadata=None))]
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
        let metadata = value.get_metadata_as_hashmap();
        let content = value.content;
        let content = unsafe { slice::from_raw_parts_mut(content.bytes, content.len) };
        let content = Vec::from(content);
        
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