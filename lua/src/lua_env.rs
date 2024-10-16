use std::collections::HashMap;

use log::error;
use mlua::{Function, Lua};

use torustiq_common::ffi::{
    shared::get_step_configuration,
    types::module::{ModuleStepHandle, Record},
};

fn torustiq_send(record: Record, step_handle: ModuleStepHandle) {
    let on_data_received_fn = match get_step_configuration(step_handle) {
        Some(c) => c.on_data_received_fn,
        None => {
            error!("torustiq_send: invalid step handle: {}", step_handle);
            return;
        }
    };

    on_data_received_fn(record.into(), step_handle);
}

/// Lua environment with pre-configured Torustiq functions
pub struct LuaEnv {
    lua: Lua,
}

impl LuaEnv {
    pub fn try_new() -> Result<Self, String> {        
        let lua = Lua::new();
        let fn_torustiq_send = match lua
            .create_function(|_, (step_handle, content, metadata):
                (ModuleStepHandle, String, HashMap<String, String>)| {
            let record = Record::from_std_types(content.as_bytes().to_vec(), metadata);
            torustiq_send(record, step_handle);
            Ok(())
        }) {
            Ok(f) => f,
            Err(e) => return Err(format!("{}", e)),
        };
        match lua.globals().set("torustiq_send", fn_torustiq_send) {
            Ok(_) => {},
            Err(e) => return Err(format!("{}", e)),
        }

        let lua_env = LuaEnv {
            lua
        };
        
        Ok(lua_env)
    }

    pub fn exec_code<S: Into<String>>(&self, code: S) -> Result<(), String> {
        match self.lua.load(code.into()).exec() {
            Ok(_) => Ok(()),
            Err(e) => Err(format!("An error occurred in Lua code: {}", e)),
        }
    }

    pub fn create_function_from_code<S: Into<String>>(&self, code: S) -> Result<Function<'_>, String> {
        match self.lua.load(code.into()).eval() {
            Ok(f) => Ok(f),
            Err(e) => Err(format!("Failed to create a function from Lua code: {}", e)),
        }
    }
    
    pub fn call_process_record_function(&self, func: &Function<'_>, step_handle: ModuleStepHandle, record: Record) -> Result<(), String> {
        match func.call::<_, String>((step_handle, record.content.to_string(), record.get_metadata_as_hashmap())) {
            Ok(_) => Ok(()), // the function needs to return something, so we return a string which is never handled
            Err(e) => Err(format!("Function call failure: {}", e))
        }
    }

}