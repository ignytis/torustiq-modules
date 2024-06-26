mod http;

use std::{collections::HashMap, sync::Mutex, thread};

use log::debug;

use once_cell::sync::Lazy;
use torustiq_common::{
    ffi::{
        types::module::{
            IoKind, ModuleInfo, ModuleProcessRecordFnResult, ModuleStepHandle, ModuleStepInitArgs, Record
        },
        utils::strings::str_to_cchar,
    },
    logging::init_logger};

static MODULE_PARAMS: Lazy<Mutex<HashMap<ModuleStepHandle, HashMap<String, String>>>> = Lazy::new(|| {
    Mutex::new(HashMap::new())
});

#[no_mangle]
pub extern "C" fn torustiq_module_get_info() -> ModuleInfo {
    ModuleInfo {
        id: str_to_cchar("source_http"),
        name: str_to_cchar("HTTP Source"),
        input_kind: IoKind::External,
        output_kind: IoKind::Stream,
    }
}

#[no_mangle]
extern "C" fn torustiq_module_init() {
    init_logger();
    debug!("Source HTTP destination: initialized");
}

#[no_mangle]
extern "C" fn torustiq_module_step_init(args: ModuleStepInitArgs) {
    let module_params_container = MODULE_PARAMS.lock().unwrap();
    let (host, port) = match module_params_container.get(&args.step_handle) {
        Some(cfg) => (cfg.get("host"), cfg.get("port")),
        None => (None, None),
    };

    let host = host.unwrap_or(&String::from("localhost")).clone();
    let port = port.unwrap_or(&String::from("8080")).clone();
    let port = port.parse::<u16>().expect(format!("Failed to parse the port number: {}", port).as_str());
    thread::spawn(move || http::run_server(args, host, port));
}

#[no_mangle]
extern "C" fn torustiq_module_process_record(_input: Record) -> ModuleProcessRecordFnResult {
    ModuleProcessRecordFnResult::None
}