mod py_env;
mod py_record;

use std::{fs, thread};

use py_env::{thread_python_env, thread_receiver, thread_sender};
use torustiq_common::{
    ffi::{
        shared::{get_param, get_pipeline_module_configuration, set_pipeline_module_configuration},
        types::module::{
            ModuleInfo, ModuleKind, ModulePipelineConfigureArgs, ModulePipelineConfigureFnResult,
            ModuleHandle, StepStartFnResult, PipelineModuleKind
        },
        utils::strings::string_to_cchar
    },
    logging::init_logger,
    pipeline::async_process,
    CURRENT_API_VERSION,
};

const MODULE_INFO: ModuleInfo = ModuleInfo {
    api_version: CURRENT_API_VERSION,
    id: c"python".as_ptr(),
    kind: ModuleKind::Pipeline,
    name: c"Python integration".as_ptr(),
};

#[no_mangle]
pub extern "C" fn torustiq_module_get_info() -> ModuleInfo {
    MODULE_INFO
}

#[no_mangle]
extern "C" fn torustiq_module_init() {
    init_logger();
}

#[no_mangle]
extern "C" fn torustiq_module_pipeline_configure(step_config: ModulePipelineConfigureArgs) -> ModulePipelineConfigureFnResult {
    // Multiple instances of module are not supported currently because of Python's GIL.
    // There is one instance of Python environment created for process, therefore threads start
    // to lock each other. Due to this reason only one instance of Python module is allowed.
    {
        let senders = async_process::RECORD_SENDERS.lock().unwrap();
        if senders.len() > 0 {
            return ModulePipelineConfigureFnResult::ErrorMultipleStepsNotSupported(*senders.keys().next().unwrap());
        }
    }

    async_process::create_sender_and_receiver(step_config.module_handle);
    set_pipeline_module_configuration(step_config);
    ModulePipelineConfigureFnResult::Ok
}

#[no_mangle]
extern "C" fn torustiq_module_common_start(handle: ModuleHandle) -> StepStartFnResult {
    let step_config = match get_pipeline_module_configuration(handle) {
        Some(c) => c,
        None => return StepStartFnResult::ErrorMisc(string_to_cchar(format!("Step '{}' has no registered configuration", handle))),
    };
    let receiver = match async_process::get_receiver_owned(handle) {
        Some(r) => r,
        None => return StepStartFnResult::ErrorMisc(string_to_cchar(format!("Step  '{}' has no registered receiver", handle))),
    };
    let code = match get_param(step_config.module_handle, "file") {
        Some(f) => {
            match fs::read_to_string(f.clone()) {
                Ok(c) => c,
                Err(e) => return StepStartFnResult::ErrorMisc(
                    string_to_cchar(format!("Failed to read contents of Python file '{}': {}", f, e)))
            }
        },
        None => match get_param(step_config.module_handle, "code_contents") {
            Some(c) => c,
            None => return StepStartFnResult::ErrorMisc(
                string_to_cchar("Either 'file' or 'code_contents' attribute must be provided for Python handler")),
        }
    };
    let module_handle = step_config.module_handle;
    let kind = step_config.kind.clone();
    thread::spawn(move || {
        match kind {
            PipelineModuleKind::Source => thread_python_env(code, thread_sender(module_handle)),
            _ => thread_python_env(code, thread_receiver(module_handle, receiver)),
        };
    });

    StepStartFnResult::Ok
}