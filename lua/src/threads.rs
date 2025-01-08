use std::time::Duration;

use log::error;

use torustiq_common::{
    ffi::types::module as module_types,
    pipeline::async_process
};

use crate::lua_env::LuaEnv;

pub struct ThreadArgs {
    pub code: String,
    pub lib_args: module_types::LibCommonInitArgs,
    pub module_args: module_types::ModulePipelineConfigureArgs,
}

/// If module is source, this thread is called.
/// Typically source code is some loop which produces new records
/// and emits them to the next step
pub fn thread_source(args: ThreadArgs) {
    let lua = match LuaEnv::try_new() {
        Ok(l) => l,
        Err(e) => {
            error!("Failed to create a Lua env in step '{}': {}", args.module_args.module_handle, e);
            (args.lib_args.on_step_terminate_cb)(args.module_args.module_handle);
            return
        },
    };

    // Launcher code: looks up the 'run(module_handle)' Lua function
    let code = format!("{}\nrun({})", args.code, args.module_args.module_handle);
    if let Err(e) = lua.exec_code(code) {
        error!("An error occurred in Lua sender code: {}", e)
    };
    (args.lib_args.on_step_terminate_cb)(args.module_args.module_handle);
}

/// Processor thread is started if the module is transformation or destination.
/// An example of simple Lua code for processor:
/// 
/// function (module_handle, content, metadata)
///   content = content .. " And hello again from Lua file!!"
///   metadata["e"] = "fgh"
///   torustiq_send(module_handle, content, metadata)
///   return ""
/// end
pub fn thread_processor(args: ThreadArgs) {
    {
        let lua = match LuaEnv::try_new() {
            Ok(l) => l,
            Err(e) => {
                error!("Failed to create a Lua env in step '{}': {}", args.module_args.module_handle, e);
                (args.lib_args.on_step_terminate_cb)(args.module_args.module_handle);
                return
            },
        };

        let rx = match async_process::get_receiver_owned(args.module_args.module_handle) {
            Some(r) => r,
            None => {
                error!("Record receiver is not registered for step '{}'", args.module_args.module_handle);
                (args.lib_args.on_step_terminate_cb)(args.module_args.module_handle);
                return
            }
        };

        let process_func = match lua.create_function_from_code(args.code) {
            Ok(f) => f,
            Err(e) => {
                error!("Failed to create a Lua function in step '{}': {}", args.module_args.module_handle, e);
                (args.lib_args.on_step_terminate_cb)(args.module_args.module_handle);
                return
            },
        };

        loop {
            let in_record: module_types::Record = match rx.recv_timeout(Duration::from_secs(1)) {
                Ok(r) => r,
                Err(_) => continue, // timeout
            }.into();
            
            if let Err(e) = lua.call_process_record_function(&process_func, args.module_args.module_handle, in_record) {
                error!("ERROR: {}", e);
            }
        }
    }
}