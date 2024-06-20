mod http;

use std::thread;

use log::debug;

use torustiq_common::{
    ffi::{
        types::module::{IoKind, ModuleInfo, ModuleInitStepArgs, ModuleProcessRecordFnResult, Record},
        utils::strings::str_to_cchar,
    },
    logging::init_logger};


#[no_mangle]
pub extern "C" fn torustiq_module_get_info() -> ModuleInfo {
    ModuleInfo {
        id: str_to_cchar("source_http"),
        name: str_to_cchar("HTTP Source"),
        input_kind: IoKind::External,
        output_kind: IoKind::Stream,
    }
}

#[no_mangle]
extern "C" fn torustiq_module_init_step(args: ModuleInitStepArgs) {
    init_logger();
    debug!("Source HTTP destination: initialized");
    thread::spawn(|| http::run_server(args));
}

#[no_mangle]
extern "C" fn torustiq_module_process_record(_input: Record) -> ModuleProcessRecordFnResult {
    ModuleProcessRecordFnResult::None
}