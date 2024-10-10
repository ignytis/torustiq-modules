mod lua_env;

use std::{collections::HashMap, sync::{mpsc::{channel, Receiver, Sender}, Mutex}, thread, time::Duration};

use log::error;
use lua_env::LuaEnv;
use once_cell::sync::Lazy;

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
        utils::strings::string_to_cchar
    },
    logging::init_logger};

const MODULE_ID: ConstCStrPtr = c"lua".as_ptr();
const MODULE_NAME: ConstCStrPtr = c"Lua".as_ptr();

static SENDERS: Lazy<Mutex<HashMap<ModuleStepHandle, Sender<Record>>>> = Lazy::new(|| {
    Mutex::new(HashMap::new())
});
static RECEIVERS: Lazy<Mutex<HashMap<ModuleStepHandle, Receiver<Record>>>> = Lazy::new(|| {
    Mutex::new(HashMap::new())
});


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
    let mut senders = SENDERS.lock().unwrap();
    let (sender, receiver) = channel::<Record>();
    RECEIVERS.lock().unwrap().insert(args.step_handle, receiver);
    senders.insert(args.step_handle, sender);

    set_step_configuration(args);
    ModuleStepConfigureFnResult::Ok
}

#[no_mangle]
extern "C" fn torustiq_module_step_start(handle: ModuleStepHandle) -> ModuleStepStartFnResult {
    let args = match get_step_configuration(handle) {
        Some(a) => a,
        None => return ModuleStepStartFnResult::ErrorMisc(string_to_cchar(format!("Init args for step '{}' not found", handle)))
    };

    // TODO: implement loading from file
    let code = get_param(handle, "code_contents").unwrap_or(String::from(""));
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

                let rx = match RECEIVERS.lock().unwrap().remove(&handle) {
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

#[no_mangle]
extern "C" fn torustiq_module_process_record(in_record: Record, step_handle: ModuleStepHandle) -> ModuleProcessRecordFnResult {
    let mutex = SENDERS.lock().unwrap();
    let sender = match mutex.get(&step_handle) {
        Some(s) => s,
        None => return ModuleProcessRecordFnResult::None,
    };
    // Cloning the record because the original record will be unallocated in main app
    sender.send(in_record.clone()).unwrap();
    ModuleProcessRecordFnResult::None
}