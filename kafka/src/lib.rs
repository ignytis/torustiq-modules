mod kafka_producer;

use std::{
    collections::HashMap, 
    sync::Mutex
};

use kafka_producer::KafkaMessage;
use log::error;
use once_cell::sync::Lazy;

use torustiq_common::{
    ffi::{
        shared::{
            get_params, get_step_configuration, set_step_configuration
        },
        types::{
            module::{
                ModuleInfo, ModuleProcessRecordFnResult, ModuleStepConfigureArgs,
                ModuleStepConfigureFnResult, ModuleStepHandle, ModuleStepStartFnResult,
                PipelineStepKind, Record
            },
            std_types::ConstCStrPtr
    },
    utils::strings::string_to_cchar},
    logging::init_logger
};
use crate::kafka_producer::KafkaProducer;

const MODULE_INFO: ModuleInfo = ModuleInfo {
    id: c"kafka".as_ptr(),
    name: c"Kafka output".as_ptr(),
};

static PRODUCER: Lazy<Mutex<Option<KafkaProducer>>> = Lazy::new(|| {
    Mutex::new(None)
});

#[no_mangle]
pub extern "C" fn torustiq_module_get_info() -> ModuleInfo {
    MODULE_INFO
}

#[no_mangle]
extern "C" fn torustiq_module_init() {
    init_logger();
}

#[no_mangle]
extern "C" fn torustiq_module_step_configure(args: ModuleStepConfigureArgs) -> ModuleStepConfigureFnResult {
    if args.kind != PipelineStepKind::Destination {
        return ModuleStepConfigureFnResult::ErrorKindNotSupported
    }
    
    set_step_configuration(args);
    ModuleStepConfigureFnResult::Ok
}

#[no_mangle]
extern "C" fn torustiq_module_step_start(handle: ModuleStepHandle) -> ModuleStepStartFnResult {
    let args = match get_step_configuration(handle) {
        Some(a) => a,
        None => return ModuleStepStartFnResult::ErrorMisc(string_to_cchar(format!("Init args for step '{}' not found", handle)))
    };

    let step_params = match get_params(args.step_handle) {
        Some(p) => p,
        None => HashMap::new(),
    };

    let driver_params: HashMap<String, String> = step_params
        .into_iter()
        .filter(|(k, _)| k.starts_with("driver."))
        .map(|(k, v)| (
            match k.strip_prefix("driver.") {
                Some(k2) => String::from(k2),
                None => k.clone()
            }, v))
        .collect();

    *PRODUCER.lock().unwrap() = Some(KafkaProducer::new(&driver_params));
    ModuleStepStartFnResult::Ok
}

#[no_mangle]
extern "C" fn torustiq_module_step_process_record(input: Record, _h: ModuleStepHandle) -> ModuleProcessRecordFnResult {
    let producer = match PRODUCER.lock().unwrap().clone() {
        Some(p) => p,
        None => {
            error!("Cannot send a message to Kafka: producer is offline");
            return ModuleProcessRecordFnResult::Ok
        }
    };
    // TODO:
    // Instead of blocking, process a message using channel
    // Consider moving the torustiq_module_step_process_record function into common module, so all modules are expected
    //   to process records in async mode (using channels)
    let mtd = input.get_metadata_as_hashmap();
    let headers: HashMap<String, String> = mtd
        .iter()
        .filter(|(k, _)| k.starts_with("kafka.headers."))
        .map(|(k, v)| (k.strip_prefix("kafka.headers.").unwrap().to_string(), v.clone()))
        .collect();
    let key = mtd.get("kafka.key").cloned();
    let topic = mtd.get("kafka.topic").unwrap_or(&String::from("test")).clone(); // TODO: handle the missing topic
    match futures::executor::block_on(producer.produce(&KafkaMessage {
        headers,
        key,
        payload: input.content.to_byte_vec(),
        topic,
    })) {
        Err(e) => print!("Failed to send a message to Kafka: {}", e),
        _ => {},
    }
    ModuleProcessRecordFnResult::Ok
}