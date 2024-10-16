mod py_env;
mod py_record;

use std::{
    collections::HashMap,
    fs,
    sync::{
        mpsc::{
            channel, Receiver, Sender
        },
        Mutex
    },
    thread
};

use log::debug;
use once_cell::sync::Lazy;

use py_env::{thread_python_env, thread_receiver, thread_sender};
use torustiq_common::{
    ffi::{
        shared::{get_param, get_step_configuration, set_step_configuration},
        types::{
            module::{
                ModuleInfo, ModuleProcessRecordFnResult, ModuleStepConfigureArgs, ModuleStepConfigureFnResult,
                ModuleStepHandle, ModuleStepStartFnResult, PipelineStepKind, Record
            },
            std_types::ConstCStrPtr
        }, utils::strings::string_to_cchar
    },
    logging::init_logger};

static SENDERS: Lazy<Mutex<HashMap<ModuleStepHandle, Sender<Record>>>> = Lazy::new(|| {
    Mutex::new(HashMap::new())
});
static RECEIVERS: Lazy<Mutex<HashMap<ModuleStepHandle, Receiver<Record>>>> = Lazy::new(|| {
    Mutex::new(HashMap::new())
});

const MODULE_ID: ConstCStrPtr = c"python".as_ptr();
const MODULE_NAME: ConstCStrPtr = c"Python integration".as_ptr();

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
    debug!("Python transform: initialized");
}

#[no_mangle]
extern "C" fn torustiq_module_step_configure(step_config: ModuleStepConfigureArgs) -> ModuleStepConfigureFnResult {
    // Multiple instances of module are not supported currently because of Python's GIL.
    // There is one instance of Python environment created for process, therefore threads start
    // to lock each other. Due to this reason only one instance of Python module is allowed.
    let mut senders = SENDERS.lock().unwrap();
    if senders.len() > 0 {
        return ModuleStepConfigureFnResult::ErrorMultipleStepsNotSupported(*senders.keys().next().unwrap());
    }
    let (sender, receiver) = channel::<Record>();
    RECEIVERS.lock().unwrap().insert(step_config.step_handle, receiver);
    senders.insert(step_config.step_handle, sender);
    set_step_configuration(step_config);

    ModuleStepConfigureFnResult::Ok
}

#[no_mangle]
extern "C" fn torustiq_module_step_start(handle: ModuleStepHandle) -> ModuleStepStartFnResult {
    let step_config = match get_step_configuration(handle) {
        Some(c) => c,
        None => return ModuleStepStartFnResult::ErrorMisc(string_to_cchar(format!("Step '{}' has no registered configuration", handle))),
    };
    let receiver = match RECEIVERS.lock().unwrap().remove(&handle) {
        Some(r) => r,
        None => return ModuleStepStartFnResult::ErrorMisc(string_to_cchar(format!("Step  '{}' has no registered receiver", handle))),
    };
    let code = match get_param(step_config.step_handle, "file") {
        Some(f) => {
            match fs::read_to_string(f.clone()) {
                Ok(c) => c,
                Err(e) => return ModuleStepStartFnResult::ErrorMisc(
                    string_to_cchar(format!("Failed to read contents of Python file '{}': {}", f, e)))
            }
        },
        None => match get_param(step_config.step_handle, "code_contents") {
            Some(c) => c,
            None => return ModuleStepStartFnResult::ErrorMisc(
                string_to_cchar("Either 'file' or 'code_contents' attribute must be provided for Python handler")),
        }
    };
    let step_handle = step_config.step_handle;
    let kind = step_config.kind.clone();
    thread::spawn(move || {
        match kind {
            PipelineStepKind::Source => thread_python_env(code, thread_sender(step_handle)),
            _ => thread_python_env(code, thread_receiver(step_handle, receiver)),
        };
    });

    ModuleStepStartFnResult::Ok
}

/// This function forwards a record received from the previous step into Python environment (see the thread_receiver function)
#[no_mangle]
extern "C" fn torustiq_module_process_record(in_record: Record, step_handle: ModuleStepHandle) -> ModuleProcessRecordFnResult {
    let mutex = SENDERS.lock().unwrap();
    let sender = match mutex.get(&step_handle) {
        Some(s) => s,
        None => return ModuleProcessRecordFnResult::Ok,
    };
    // Cloning the record because the original record will be unallocated in main app
    sender.send(in_record.clone()).unwrap();
    ModuleProcessRecordFnResult::Ok
}