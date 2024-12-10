mod lua_env;

use std::{fs, thread, time::Duration};

use log::error;
use lua_env::LuaEnv;

use torustiq_common::{
    ffi::{
        shared::{get_param, get_pipeline_module_configuration, set_pipeline_module_configuration},
        types::module::{
            ModuleInfo, ModuleKind, ModulePipelineConfigureArgs, ModulePipelineConfigureFnResult,
            ModuleHandle, StepStartFnResult, PipelineModuleKind, Record
        },
        utils::strings::string_to_cchar
    },
    logging::init_logger,
    pipeline::async_process::{self, create_sender_and_receiver},
    CURRENT_API_VERSION,
};

const MODULE_INFO: ModuleInfo = ModuleInfo {
    api_version: CURRENT_API_VERSION,
    id: c"lua".as_ptr(),
    kind: ModuleKind::Pipeline,
    name: c"Lua".as_ptr(),
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
    match args.kind {
        PipelineModuleKind::Source => {
            thread::spawn(move || {
                let lua = match LuaEnv::try_new() {
                    Ok(l) => l,
                    Err(e) => {
                        error!("Failed to create a Lua env in step '{}': {}", handle, e);
                        (args.on_step_terminate_cb)(args.module_handle);
                        return
                    },
                };

                // Launcher codes: looks up the 'run(module_handle)' Lua function
                let code = format!("{}\nrun({})", code, handle);
                if let Err(e) = lua.exec_code(code) {
                    error!("An error occurred in Lua sender code: {}", e)
                };
                (args.on_step_terminate_cb)(args.module_handle);
            });
        },
        _ => {
            thread::spawn(move || {
                let lua = match LuaEnv::try_new() {
                    Ok(l) => l,
                    Err(e) => {
                        error!("Failed to create a Lua env in step '{}': {}", handle, e);
                        (args.on_step_terminate_cb)(args.module_handle);
                        return
                    },
                };

                let rx = match async_process::get_receiver_owned(handle) {
                    Some(r) => r,
                    None => {
                        error!("Record receiver is not registered for step '{}'", handle);
                        (args.on_step_terminate_cb)(args.module_handle);
                        return
                    }
                };

                let process_func = match lua.create_function_from_code(code) {
                    Ok(f) => f,
                    Err(e) => {
                        error!("Failed to create a Lua function in step '{}': {}", handle, e);
                        (args.on_step_terminate_cb)(args.module_handle);
                        return
                    },
                };

                loop {
                    let in_record: Record = match rx.recv_timeout(Duration::from_secs(1)) {
                        Ok(r) => r,
                        Err(_) => continue, // timeout
                    }.into();
                    
                    if let Err(e) = lua.call_process_record_function(&process_func, handle, in_record) {
                        error!("ERROR: {}", e);
                    };
                }
            });
        },
    };
    StepStartFnResult::Ok
}