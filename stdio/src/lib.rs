use std::{thread, time::Duration};

use log::{debug, error};

use torustiq_common::{
    ffi::{
        shared::{get_param, get_pipeline_module_configuration, set_pipeline_module_configuration},
        types::{
            buffer::ByteBuffer, collections::Array,
            module::{
                ModuleInfo, ModuleKind, ModulePipelineConfigureArgs, ModulePipelineConfigureFnResult,
                ModuleHandle, StepStartFnResult, PipelineModuleKind, Record
            },
        },
        utils::strings::{bytes_to_string_safe, cchar_to_string, string_to_cchar}
    },
    logging::init_logger, pipeline::async_process,
    CURRENT_API_VERSION};

const MODULE_INFO: ModuleInfo = ModuleInfo {
    api_version: CURRENT_API_VERSION,
    id: c"stdio".as_ptr(),
    kind: ModuleKind::Pipeline,
    name: c"Standard Input and Output".as_ptr(),
};

#[no_mangle]
pub extern "C" fn torustiq_module_get_info() -> ModuleInfo {
    MODULE_INFO
}

#[no_mangle]
extern "C" fn torustiq_module_init() {
    init_logger();
}

#[no_mangle]
extern "C" fn torustiq_module_pipeline_configure(args: ModulePipelineConfigureArgs) -> ModulePipelineConfigureFnResult {
    if !(args.kind == PipelineModuleKind::Source || args.kind == PipelineModuleKind::Destination) {
        return ModulePipelineConfigureFnResult::ErrorKindNotSupported;
    };
    async_process::create_sender_and_receiver(args.module_handle);
    set_pipeline_module_configuration(args);
    ModulePipelineConfigureFnResult::Ok
}

#[no_mangle]
extern "C" fn torustiq_module_common_start(handle: ModuleHandle) -> StepStartFnResult {
    let args = match get_pipeline_module_configuration(handle) {
        Some(a) => a,
        None => return StepStartFnResult::ErrorMisc(string_to_cchar(format!("Init args for step '{}' not found", handle)))
    };

    match args.kind {
        PipelineModuleKind::Source => {
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
                    (args.on_data_receive_cb)(r, args.module_handle);
                }
                debug!("End of stdin is reached. Terminating the stdin source...");
                (args.on_step_terminate_cb)(args.module_handle);
            });
            StepStartFnResult::Ok
        },
        PipelineModuleKind::Destination => {
            thread::spawn(move || {
                let tpl = get_param(args.module_handle, "format")
                    .unwrap_or(String::from("%R"));

                let rx = match async_process::get_receiver_owned(handle) {
                    Some(r) => r,
                    None => {
                        error!("Record receiver is not registered for step '{}'", handle);
                        (args.on_step_terminate_cb)(args.module_handle);
                        return
                    }
                };
                loop {
                    let input: Record = match rx.recv_timeout(Duration::from_secs(1)) {
                        Ok(r) => r,
                        Err(_) => continue, // timeout
                    }.into();

                    let content = bytes_to_string_safe(input.content.bytes, input.content.len);
                    let mtd_len = input.metadata.len as usize;
                    let metadata = unsafe { Vec::from_raw_parts(input.metadata.data, mtd_len, mtd_len) }
                        .into_iter()
                        .map(|metadata_record| vec![cchar_to_string(metadata_record.name), cchar_to_string(metadata_record.value)].join(" = "))
                        .collect::<Vec<String>>()
                        .join(", ");

                    let out_str = tpl
                        .replace("%R", content.as_str())
                        .replace("%M", metadata.as_str());
                    println!("{}", out_str);
                }
            });
            StepStartFnResult::Ok
        },
        _ => StepStartFnResult::ErrorMisc(string_to_cchar(format!("Step '{}' must be either source or destination", handle))),
    }
}