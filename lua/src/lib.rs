mod lua_env;

use std::thread;

use log::error;
use lua_env::LuaEnv;

use torustiq_common::{
    ffi::{
        shared::{get_param, set_step_configuration, get_step_configuration},
        types::{
            module::{
                ModuleInfo, ModuleProcessRecordFnResult, ModuleStepConfigureArgs, ModuleStepConfigureFnResult,
                ModuleStepHandle, ModuleStepStartFnResult, PipelineStepKind, Record
            },
            std_types::ConstCStrPtr
        },
        utils::strings::{bytes_to_string_safe, cchar_to_string, string_to_cchar}
    },
    logging::init_logger};

const MODULE_ID: ConstCStrPtr = c"lua".as_ptr();
const MODULE_NAME: ConstCStrPtr = c"Lua".as_ptr();


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
}

#[no_mangle]
extern "C" fn torustiq_module_step_configure(args: ModuleStepConfigureArgs) -> ModuleStepConfigureFnResult {
    match args.kind {
        PipelineStepKind::Source => {},
        _ => return ModuleStepConfigureFnResult::ErrorKindNotSupported,
    };
    set_step_configuration(args);
    ModuleStepConfigureFnResult::Ok
}

#[no_mangle]
extern "C" fn torustiq_module_step_start(handle: ModuleStepHandle) -> ModuleStepStartFnResult {
    let args = match get_step_configuration(handle) {
        Some(a) => a,
        None => return ModuleStepStartFnResult::ErrorMisc(string_to_cchar(format!("Init args for step '{}' not found", handle)))
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

                // TODO: implement loading from file
                let code = get_param(handle, "code_contents").unwrap_or(String::from(""));
                // Launcher codes: looks up the 'run(step_handle)' Lua function
                let code = format!("{}\nrun({})", code, handle);
                match lua.exec_code(code) {
                    Ok(_) => {},
                    Err(e) => error!("An error occurred in Lua code: {}", e)
                };
                (args.on_step_terminate_cb)(args.step_handle);
            });
            ModuleStepStartFnResult::Ok
        },
        _ => ModuleStepStartFnResult::ErrorMisc(string_to_cchar(format!("Step '{}' must be source", handle))),
    }
}

#[no_mangle]
extern "C" fn torustiq_module_process_record(input: Record, h: ModuleStepHandle) -> ModuleProcessRecordFnResult {
    let content = bytes_to_string_safe(input.content.bytes, input.content.len);
    let mtd_len = input.metadata.len as usize;
    let metadata = unsafe { Vec::from_raw_parts(input.metadata.data, mtd_len, mtd_len) }
        .into_iter()
        .map(|metadata_record| vec![cchar_to_string(metadata_record.name), cchar_to_string(metadata_record.value)].join(" = "))
        .collect::<Vec<String>>()
        .join(", ");

    let out_str = get_param(h, "format")
        .unwrap_or(String::from("%R"))
        .replace("%R", content.as_str())
        .replace("%M", metadata.as_str());

    println!("{}", out_str);
    ModuleProcessRecordFnResult::None
}