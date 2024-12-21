use std::{
    collections::HashMap,
    sync::Mutex, thread::sleep,
    time::Duration
};
use once_cell::sync::Lazy;

use torustiq_common::ffi::{
    shared::get_params,
    types::module as module_types,
};

pub static STATS_MSG_IN: Lazy<Mutex<HashMap<String, usize>>> = Lazy::new(|| {
    Mutex::new(HashMap::new())
});
pub static STATS_MSG_OUT: Lazy<Mutex<HashMap<String, usize>>> = Lazy::new(|| {
    Mutex::new(HashMap::new())
});
pub static STATS_ERRORS_NUM: Lazy<Mutex<HashMap<String, usize>>> = Lazy::new(|| {
    Mutex::new(HashMap::new())
});

/// Initializes metrics
pub fn init(handle: module_types::ModuleHandle) -> Result<(), String> {
        let params = get_params(handle).unwrap_or(HashMap::new());
        let mut stats_msg_in = match STATS_MSG_IN.lock() {
            Ok(s) => s,
            Err(e) => return Err(format!("Failed to obtain a pointer to MSG_IN stats: {}", e)),
        };
        let mut stats_msg_out = match STATS_MSG_OUT.lock() {
            Ok(s) => s,
            Err(e) => return Err(format!("Failed to obtain a pointer to MSG_OUT stats: {}", e)),
        };
        let mut stats_errors_num = match STATS_ERRORS_NUM.lock() {
            Ok(s) => s,
            Err(e) => return Err(format!("Failed to obtain a pointer to ERRORS_NUM stats: {}", e)),
        };

        params.iter().for_each(|(k, v)| {
            if !(k.starts_with("pipeline.steps.") && k.ends_with(".id")) {
                return
            }
            stats_msg_in.insert(v.clone(), 0);
            stats_msg_out.insert(v.clone(), 0);
            stats_errors_num.insert(v.clone(), 0);
        });
    Ok(())
}

fn update_gauge(mtx: &Lazy<Mutex<HashMap<String, usize>>>, metric_name: String, step_id: &String) {
    let hashmap = match mtx.lock() {
        Ok(s) => s,
        Err(e) => {
            log::error!("Failed to obtain a pointer to {} stats: {}", metric_name, e);
            return
        },
    };
    let val = *hashmap.get(step_id).unwrap_or(&0) as f64;
    metrics::gauge!(metric_name, "step_id" => step_id.clone()).set(val);
}

/// A thread routine which periodically refreshes metrics on HTTP listener
pub fn thread_refresh() {
    let step_ids: Vec<String> = {
        let stats_msg_in = match STATS_MSG_IN.lock() {
            Ok(s) => s,
            Err(e) => {
                log::error!("Failed to obtain a pointer to MSG_IN stats: {}. The Metrics thread will be stoped.", e);
                return
            },
        };
        stats_msg_in.clone().into_keys().collect()
    };
    loop {
        for step_id in &step_ids {
            update_gauge(&STATS_MSG_IN, String::from("pipeline_steps_msg_in"), step_id);
            update_gauge(&STATS_MSG_OUT, String::from("pipeline_steps_msg_out"), step_id);
            update_gauge(&STATS_ERRORS_NUM, String::from("pipeline_steps_errors_num"), step_id);
        }
        sleep(Duration::from_secs(5));
    }
}