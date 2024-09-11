use std::thread;

use log::error;

use torustiq_common::{
    ffi::{
        shared::get_param, types::{
            buffer::ByteBuffer, collections::Array,
            module::{
                ModuleInfo, ModuleProcessRecordFnResult, ModuleStepHandle,
                ModuleStepConfigureArgs, ModuleStepConfigureFnResult, PipelineStepKind, Record
            },
            std_types::ConstCStrPtr
        },
        utils::strings::{bytes_to_string_safe, cchar_to_string}
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
extern "C" fn torustiq_module_step_configure(args: ModuleStepConfigureArgs) -> ModuleStepConfigureFnResult {
    match args.kind {
        PipelineStepKind::Source => {
            thread::spawn(move || {
                let stdin = std::io::stdin();
                let mut lines = stdin.lines();
                while let Some(line) = lines.next() {
                    let content = match line {
                        Ok(l) => l,
                        Err(e) => {
                            error!("Error on reading a line: {}", e);
                            break
                        },
                    };
                    let r = Record {
                        content: ByteBuffer::from(content),
                        metadata: Array::new_of_len(0),
                    };
                    (args.on_data_received_fn)(r, args.step_handle);

                }
                (args.termination_handler)(args.step_handle);
            });
            ModuleStepConfigureFnResult::Ok
        },
        PipelineStepKind::Destination => ModuleStepConfigureFnResult::Ok,
        _ => ModuleStepConfigureFnResult::ErrorKindNotSupported,
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