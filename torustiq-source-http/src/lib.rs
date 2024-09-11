mod http;

use std::{collections::HashMap, sync::Mutex, thread};

use log::debug;

use once_cell::sync::Lazy;
use torustiq_common::{
    ffi::types::{module::{
            ModuleInfo, ModuleStepConfigureFnResult, ModuleProcessRecordFnResult, ModuleStepHandle,
            ModuleStepConfigureArgs, PipelineStepKind, Record
        }, std_types::ConstCStrPtr},
    logging::init_logger};

static MODULE_PARAMS: Lazy<Mutex<HashMap<ModuleStepHandle, HashMap<String, String>>>> = Lazy::new(|| {
    Mutex::new(HashMap::new())
});

const MODULE_ID: ConstCStrPtr = c"source_http".as_ptr();
const MODULE_NAME: ConstCStrPtr = c"HTTP Source".as_ptr();

#[no_mangle]
pub extern "C" fn torustiq_module_get_info() -> ModuleInfo {
    ModuleInfo {
        id: MODULE_ID,
        name: MODULE_NAME,
    }
}

#[no_mangle]
extern "C" fn torustiq_module_init() {
    init_logger();
    debug!("Source HTTP destination: initialized");
}

#[no_mangle]
extern "C" fn torustiq_module_step_configure(args: ModuleStepConfigureArgs) -> ModuleStepConfigureFnResult {
    if args.kind != PipelineStepKind::Source {
        return ModuleStepConfigureFnResult::ErrorKindNotSupported;
    }

    let module_params_container = MODULE_PARAMS.lock().unwrap();
    let (host, port) = match module_params_container.get(&args.step_handle) {
        Some(cfg) => (cfg.get("host"), cfg.get("port")),
        None => (None, None),
    };

    let host = host.unwrap_or(&String::from("localhost")).clone();
    let port = port.unwrap_or(&String::from("8080")).clone();
    let port = port.parse::<u16>().expect(format!("Failed to parse the port number: {}", port).as_str());
    thread::spawn(move || http::run_server(args, host, port));
    ModuleStepConfigureFnResult::Ok
}

#[no_mangle]
extern "C" fn torustiq_module_process_record(_input: Record, _h: ModuleStepHandle) -> ModuleProcessRecordFnResult {
    ModuleProcessRecordFnResult::None
}