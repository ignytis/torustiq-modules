use std::collections::HashMap;

use log::debug;

use torustiq_common::{
    ffi::{
        types::{module::{
            IoKind, ModuleInfo, ModuleProcessRecordFnResult, ModuleStepInitArgs, Record}, std_types::ConstCStrPtr},
        utils::strings::{bytes_to_string_safe, cchar_to_string},
    },
    logging::init_logger};

const MODULE_ID: ConstCStrPtr = c"destination_stdout".as_ptr();
const MODULE_NAME: ConstCStrPtr = c"STDOUT destination".as_ptr();

#[no_mangle]
pub extern "C" fn torustiq_module_get_info() -> ModuleInfo {
    ModuleInfo {
        id: MODULE_ID,
        name: MODULE_NAME,
        input_kind: IoKind::Stream,
        output_kind: IoKind::External,
    }
}

#[no_mangle]
extern "C" fn torustiq_module_init() {
    init_logger();
    debug!("Source HTTP destination: initialized");
}

#[no_mangle]
extern "C" fn torustiq_module_step_init(_args: ModuleStepInitArgs) {

}

#[no_mangle]
extern "C" fn torustiq_module_process_record(input: Record) -> ModuleProcessRecordFnResult {
    let content = bytes_to_string_safe(input.content.bytes, input.content.len);
    // let output = "Record: ";
    let mtd_len = input.metadata.len as usize;
    let metadata = unsafe { Vec::from_raw_parts(input.metadata.data, mtd_len, mtd_len) }
        .into_iter()
        .map(|record| vec![cchar_to_string(record.name), cchar_to_string(record.value)].join(" = "))
        .collect::<Vec<String>>()
        .join(", ");

    println!("Record: {}\nMtd: {}", content, metadata);
    ModuleProcessRecordFnResult::None
}