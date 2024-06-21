use torustiq_common::{
    ffi::{
        types::module::{IoKind, ModuleInfo, ModuleInitStepArgs, ModuleProcessRecordFnResult, Record},
        utils::strings::{cchar_to_string, str_to_cchar},
    },
    logging::init_logger};

#[no_mangle]
pub extern "C" fn torustiq_module_get_info() -> ModuleInfo {
    ModuleInfo {
        id: str_to_cchar("destination_stdout"),
        name: str_to_cchar("STDOUT destination"),
        input_kind: IoKind::Stream,
        output_kind: IoKind::External,
    }
}

#[no_mangle]
extern "C" fn torustiq_module_init_step(_args: ModuleInitStepArgs) {
    init_logger();
}

#[no_mangle]
extern "C" fn torustiq_module_process_record(input: Record) -> ModuleProcessRecordFnResult {
    let p = cchar_to_string(input.content.get_bytes_as_const_ptr());
    println!("{}", p);
    ModuleProcessRecordFnResult::None
}