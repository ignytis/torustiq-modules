use torustiq_common::{
    ffi::{
        shared::get_param, types::{module::{ModuleInfo, ModuleProcessRecordFnResult, ModuleStepHandle, ModuleStepInitArgs, ModuleStepInitFnResult, PipelineStepKind, Record},
        std_types::ConstCStrPtr}, utils::strings::{bytes_to_string_safe, cchar_to_string}
    },
    logging::init_logger};

const MODULE_ID: ConstCStrPtr = c"stdio".as_ptr();
const MODULE_NAME: ConstCStrPtr = c"Standard Input and Output".as_ptr();

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
extern "C" fn torustiq_module_step_init(args: ModuleStepInitArgs) -> ModuleStepInitFnResult {
    match args.kind {
        PipelineStepKind::Destination => ModuleStepInitFnResult::Ok,
        _ => ModuleStepInitFnResult::ErrorKindNotSupported,
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