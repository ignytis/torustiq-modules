mod lua_env;

use std::{fs, thread, time::Duration};

use log::error;
use lua_env::LuaEnv;

use torustiq_common::{
    ffi::{
        shared::{get_param, get_step_configuration, set_step_configuration},
        types::module::{
            ModuleInfo, ModuleKind, ModuleStepConfigureArgs, ModuleStepConfigureFnResult,
            ModuleStepHandle, ModuleStepStartFnResult, PipelineStepKind, Record
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
    kind: ModuleKind::Step,
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
extern "C" fn torustiq_module_step_configure(args: ModuleStepConfigureArgs) -> ModuleStepConfigureFnResult {    
    create_sender_and_receiver(args.step_handle);
    set_step_configuration(args);
    ModuleStepConfigureFnResult::Ok
}

#[no_mangle]
extern "C" fn torustiq_module_step_start(handle: ModuleStepHandle) -> ModuleStepStartFnResult {
    let args = match get_step_configuration(handle) {
        Some(a) => a,
        None => return ModuleStepStartFnResult::ErrorMisc(string_to_cchar(format!("Init args for step '{}' not found", handle)))
    };

    let code = match get_param(handle, "file") {
        Some(f) => {
            match fs::read_to_string(f.clone()) {
                Ok(c) => c,
                Err(e) => return ModuleStepStartFnResult::ErrorMisc(
                    string_to_cchar(format!("Failed to read contents of Lua file '{}': {}", f, e)))
            }
        },
        None => match get_param(handle, "code_contents") {
            Some(c) => c,
            None => return ModuleStepStartFnResult::ErrorMisc(
                string_to_cchar("Either 'file' or 'code_contents' attribute must be provided for Lua handler")),
        }
    };
    match args.kind {
        PipelineStepKind::Source => {
            thread::spawn(move || {
                let lua = match LuaEnv::try_new() {
                    Ok(l) => l,
                    Err(e) => {
                        error!("Failed to create a Lua env in step '{}': {}", handle, e);
                        (args.on_step_terminate_cb)(args.step_handle);
                        return
                    },
                };

                // Launcher codes: looks up the 'run(step_handle)' Lua function
                let code = format!("{}\nrun({})", code, handle);
                match lua.exec_code(code) {
                    Ok(_) => {},
                    Err(e) => error!("An error occurred in Lua sender code: {}", e)
                };
                (args.on_step_terminate_cb)(args.step_handle);
            });
            
        },
        _ => {
            thread::spawn(move || {
                let lua = match LuaEnv::try_new() {
                    Ok(l) => l,
                    Err(e) => {
                        error!("Failed to create a Lua env in step '{}': {}", handle, e);
                        (args.on_step_terminate_cb)(args.step_handle);
                        return
                    },
                };

                let rx = match async_process::get_receiver_owned(handle) {
                    Some(r) => r,
                    None => {
                        error!("Record receiver is not registered for step '{}'", handle);
                        (args.on_step_terminate_cb)(args.step_handle);
                        return
                    }
                };

                let process_func = match lua.create_function_from_code(code) {
                    Ok(f) => f,
                    Err(e) => {
                        error!("Failed to create a Lua function in step '{}': {}", handle, e);
                        (args.on_step_terminate_cb)(args.step_handle);
                        return
                    },
                };

                loop {
                    let in_record: Record = match rx.recv_timeout(Duration::from_secs(1)) {
                        Ok(r) => r,
                        Err(_) => continue, // timeout
                    }.into();
                    
                    match lua.call_process_record_function(&process_func, handle, in_record) {
                        Ok(_) => {},
                        Err(e) => error!("ERROR: {}", e),
                    };
                }
            });
        },
    };
    ModuleStepStartFnResult::Ok
}