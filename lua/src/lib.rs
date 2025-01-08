mod lua_env;
mod threads;

use std::{fs, thread};

use torustiq_common::{
    ffi::{
        shared::{get_common_lib_configuration, get_param, get_pipeline_module_configuration, set_pipeline_module_configuration},
        types::module::{
            LibInfo, ModuleHandle, ModuleKind, ModulePipelineConfigureArgs,
            ModulePipelineConfigureFnResult, PipelineModuleKind, StepStartFnResult
        },
        utils::strings::string_to_cchar
    },
    pipeline::async_process::create_sender_and_receiver,
    CURRENT_API_VERSION,
};

const MODULE_INFO: LibInfo = LibInfo {
    api_version: CURRENT_API_VERSION,
    id: c"lua".as_ptr(),
    kind: ModuleKind::Pipeline,
    name: c"Lua".as_ptr(),
};

#[no_mangle]
pub extern "C" fn torustiq_module_get_info() -> LibInfo {
    MODULE_INFO
}

#[no_mangle]
extern "C" fn torustiq_module_pipeline_configure(args: ModulePipelineConfigureArgs) -> ModulePipelineConfigureFnResult {
    create_sender_and_receiver(args.module_handle);
    set_pipeline_module_configuration(args);
    ModulePipelineConfigureFnResult::Ok
}

#[no_mangle]
extern "C" fn torustiq_module_common_start(handle: ModuleHandle) -> StepStartFnResult {
    let args = match get_pipeline_module_configuration(handle) {
        Some(a) => a,
        None => return StepStartFnResult::ErrorMisc(string_to_cchar(format!("Init args for step '{}' not found", handle)))
    };
    let lib_args = get_common_lib_configuration().unwrap();

    let code = match get_param(handle, "file") {
        Some(f) => {
            match fs::read_to_string(f.clone()) {
                Ok(c) => c,
                Err(e) => return StepStartFnResult::ErrorMisc(
                    string_to_cchar(format!("Failed to read contents of Lua file '{}': {}", f, e)))
            }
        },
        None => match get_param(handle, "code_contents") {
            Some(c) => c,
            None => return StepStartFnResult::ErrorMisc(
                string_to_cchar("Either 'file' or 'code_contents' attribute must be provided for Lua handler")),
        }
    };
    let args_kind = args.kind.clone();
    let thread_args = threads::ThreadArgs {
        code,
        lib_args,
        module_args: args,
    };
    match args_kind {
        PipelineModuleKind::Source => thread::spawn(|| threads::thread_source(thread_args)),
        _ => thread::spawn(|| threads::thread_processor(thread_args)),
    };
    StepStartFnResult::Ok
}