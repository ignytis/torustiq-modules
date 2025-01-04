mod http;

use std::{collections::HashMap, sync::Mutex, thread};

use log::debug;

use once_cell::sync::Lazy;
use torustiq_common::{
    ffi::{
        shared::{
            get_pipeline_module_configuration,
            set_pipeline_module_configuration
        },
        types::module::{
            LibInfo, ModuleKind, ModulePipelineProcessRecordFnResult, ModulePipelineConfigureArgs, ModulePipelineConfigureFnResult,
            ModuleHandle, StepStartFnResult, PipelineModuleKind, Record
        },
        utils::strings::string_to_cchar
    },
    logging::init_logger,
    CURRENT_API_VERSION
};

static MODULE_PARAMS: Lazy<Mutex<HashMap<ModuleHandle, HashMap<String, String>>>> = Lazy::new(|| {
    Mutex::new(HashMap::new())
});

const MODULE_INFO: LibInfo = LibInfo {
    api_version: CURRENT_API_VERSION,
    id: c"source_http".as_ptr(),
    kind: ModuleKind::Pipeline,
    name: c"HTTP Source".as_ptr(),
};

#[no_mangle]
pub extern "C" fn torustiq_module_get_info() -> LibInfo {
    MODULE_INFO
}

#[no_mangle]
extern "C" fn torustiq_module_init() {
    init_logger();
    debug!("Source HTTP destination: initialized");
}

#[no_mangle]
extern "C" fn torustiq_module_pipeline_configure(args: ModulePipelineConfigureArgs) -> ModulePipelineConfigureFnResult {
    if args.kind != PipelineModuleKind::Source {
        return ModulePipelineConfigureFnResult::ErrorKindNotSupported;
    }

    set_pipeline_module_configuration(args);
    ModulePipelineConfigureFnResult::Ok
}

#[no_mangle]
extern "C" fn torustiq_module_common_start(handle: ModuleHandle) -> StepStartFnResult {
    let args = match get_pipeline_module_configuration(handle) {
        Some(a) => a,
        None => return StepStartFnResult::ErrorMisc(string_to_cchar(format!("Init args for step '{}' not found", handle)))
    };

    let module_params_container = MODULE_PARAMS.lock().unwrap();
    let (host, port) = match module_params_container.get(&args.module_handle) {
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
extern "C" fn torustiq_module_pipeline_process_record(_input: Record, _h: ModuleHandle) -> ModulePipelineProcessRecordFnResult {
    ModulePipelineProcessRecordFnResult::Ok
}