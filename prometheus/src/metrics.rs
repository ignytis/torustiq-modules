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

/// Key: module handle
/// Value:
/// 1. Module ID
/// 2. Metric value
type GaugeContainer = Lazy<Mutex<HashMap<module_types::ModuleHandle, (String, usize)>>>;

pub static STATS_MSG_IN: GaugeContainer = Lazy::new(|| {
    Mutex::new(HashMap::new())
});
pub static STATS_MSG_OUT: GaugeContainer = Lazy::new(|| {
    Mutex::new(HashMap::new())
});
pub static STATS_ERRORS_NUM: GaugeContainer = Lazy::new(|| {
    Mutex::new(HashMap::new())
});

/// Initializes metrics
pub fn init(handle: module_types::ModuleHandle) -> Result<(), String> {
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

    let pipeline_step_cfg: HashMap<String, String> = get_params(handle)
        .unwrap_or(HashMap::new())
        .iter()
        .filter_map(|(k, v)| match k.starts_with("pipeline.steps.") {
            true => Some((String::from(k.strip_prefix("pipeline.steps.").unwrap()), v.clone())),
            false => None,
        })
        .collect();
    // Key: pipeline.steps.<this>.some.sub.param'. Supposed to be handle, but not necessarily
    // Value (handle, module ID)
    let mut pipeline_step_cfg_parsed: HashMap<String, (module_types::ModuleHandle, String)> = HashMap::default();
    for (k, v) in pipeline_step_cfg {
        let key = match k.find(".") {
            Some(p) => String::from(&k[..p]),
            None => continue
        };
        if !pipeline_step_cfg_parsed.contains_key(&key) {
            pipeline_step_cfg_parsed.insert(key.clone(), (0, String::default()));
        }

        let parsed_v = pipeline_step_cfg_parsed.get_mut(&key).unwrap();
        if k.ends_with(".handle") {
            parsed_v.0 = v.parse::<u32>().unwrap();
        } else if k.ends_with(".id") {
            parsed_v.1 = v;
        }
    }

    pipeline_step_cfg_parsed.iter().for_each(|(_k, (step_handle, step_id))| {
        stats_msg_in.insert(*step_handle, (step_id.clone(), 0));
        stats_msg_out.insert(*step_handle, (step_id.clone(), 0));
        stats_errors_num.insert(*step_handle, (step_id.clone(), 0));
    });
    Ok(())
}

fn update_gauge(mtx: &GaugeContainer, metric_name: String, handle: module_types::ModuleHandle) {
    let hashmap = match mtx.lock() {
        Ok(s) => s,
        Err(e) => {
            log::error!("Failed to obtain a pointer to {} stats: {}", metric_name, e);
            return
        },
    };
    let (step_id, val) = match hashmap.get(&handle) {
        Some(v) => v,
        None => return
    };
    metrics::gauge!(metric_name, "step_id" => step_id.clone()).set(*val as  f64);
}

/// A thread routine which periodically refreshes metrics on HTTP listener
pub fn thread_refresh() {
    let handles: Vec<module_types::ModuleHandle> = {
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
        for handle in &handles {
            update_gauge(&STATS_MSG_IN, String::from("pipeline_steps_msg_in"), *handle);
            update_gauge(&STATS_MSG_OUT, String::from("pipeline_steps_msg_out"), *handle);
            update_gauge(&STATS_ERRORS_NUM, String::from("pipeline_steps_errors_num"), *handle);
        }
        sleep(Duration::from_secs(5));
    }
}

pub fn inc_stats_msg_in(handle: module_types::ModuleHandle) {
    let mut hashmap = STATS_MSG_IN.lock().unwrap();
    let (step_id, num) = match hashmap.get(&handle) {
        Some(v) => (v.0.clone(), v.1),
        None => return
    };
    hashmap.insert(handle, (step_id.clone(), num + 1));
}

pub fn inc_stats_msg_out(handle: module_types::ModuleHandle) {
    let mut hashmap = STATS_MSG_OUT.lock().unwrap();
    let (step_id, num) = match hashmap.get(&handle) {
        Some(v) => (v.0.clone(), v.1),
        None => return
    };
    hashmap.insert(handle, (step_id.clone(), num + 1));
}

pub fn inc_stats_errors_num(handle: module_types::ModuleHandle) {
    let mut hashmap = STATS_ERRORS_NUM.lock().unwrap();
    let (step_id, num) = match hashmap.get(&handle) {
        Some(v) => (v.0.clone(), v.1),
        None => return
    };
    hashmap.insert(handle, (step_id.clone(), num + 1));
}