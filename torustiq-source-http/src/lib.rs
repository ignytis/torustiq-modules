mod http;

use std::{collections::HashMap, sync::Mutex, thread};

use log::debug;

use once_cell::sync::Lazy;
use torustiq_common::{
    ffi::{
        shared::{
            get_step_configuration,
            set_step_configuration
        },
        types::module::{
            ModuleInfo, ModuleKind, ModuleProcessRecordFnResult, ModulePipelineStepConfigureArgs, ModuleStepConfigureFnResult,
            ModuleStepHandle, StepStartFnResult, PipelineStepKind, Record
        },
        utils::strings::string_to_cchar
    },
    logging::init_logger,
    CURRENT_API_VERSION
};

static MODULE_PARAMS: Lazy<Mutex<HashMap<ModuleStepHandle, HashMap<String, String>>>> = Lazy::new(|| {
    Mutex::new(HashMap::new())
});

const MODULE_INFO: ModuleInfo = ModuleInfo {
    api_version: CURRENT_API_VERSION,
    id: c"source_http".as_ptr(),
    kind: ModuleKind::Pipeline,
    name: c"HTTP Source".as_ptr(),
};

#[no_mangle]
pub extern "C" fn torustiq_module_get_info() -> ModuleInfo {
    MODULE_INFO
}

#[no_mangle]
extern "C" fn torustiq_module_init() {
    init_logger();
    debug!("Source HTTP destination: initialized");
}

#[no_mangle]
extern "C" fn torustiq_module_step_configure(args: ModulePipelineStepConfigureArgs) -> ModuleStepConfigureFnResult {
    if args.kind != PipelineStepKind::Source {
        return ModuleStepConfigureFnResult::ErrorKindNotSupported;
    }

    set_step_configuration(args);
    ModuleStepConfigureFnResult::Ok
}

#[no_mangle]
extern "C" fn torustiq_module_step_start(handle: ModuleStepHandle) -> StepStartFnResult {
    let args = match get_step_configuration(handle) {
        Some(a) => a,
        None => return StepStartFnResult::ErrorMisc(string_to_cchar(format!("Init args for step '{}' not found", handle)))
    };

    let module_params_container = MODULE_PARAMS.lock().unwrap();
    let (host, port) = match module_params_container.get(&args.step_handle) {
        Some(cfg) => (cfg.get("host"), cfg.get("port")),
        None => (None, None),
    };

    let host = host.unwrap_or(&String::from("localhost")).clone();
    let port = port.unwrap_or(&String::from("8080")).clone();
    let port = port.parse::<u16>().expect(format!("Failed to parse the port number: {}", port).as_str());
    thread::spawn(move || http::run_server(args, host, port));
    StepStartFnResult::Ok
}

/// Do nothing. The module is not supposed to process records from previous steps
#[no_mangle]
extern "C" fn torustiq_module_step_process_record(_input: Record, _h: ModuleStepHandle) -> ModuleProcessRecordFnResult {
    ModuleProcessRecordFnResult::Ok
}