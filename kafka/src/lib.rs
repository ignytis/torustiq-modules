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
            get_params, get_pipeline_module_configuration, set_pipeline_lib_configuration, set_pipeline_module_configuration
        },
        types::{
            module as module_types,
            std_types::ConstCStrPtr
    },
    utils::strings::string_to_cchar},
    logging::init_logger
};
use crate::kafka_producer::KafkaProducer;

const MODULE_INFO: LibInfo = LibInfo {
    id: c"kafka".as_ptr(),
    name: c"Kafka output".as_ptr(),
};

static PRODUCER: Lazy<Mutex<Option<KafkaProducer>>> = Lazy::new(|| {
    Mutex::new(None)
});

#[no_mangle]
pub extern "C" fn torustiq_module_get_info() -> LibInfo {
    MODULE_INFO
}

#[no_mangle]
extern "C" fn torustiq_module_init(a: module_types::LibPipelineInitArgs) {
    set_pipeline_lib_configuration(a);
    init_logger();
}

#[no_mangle]
extern "C" fn torustiq_module_pipeline_configure(args: ModulePipelineConfigureArgs) -> ModulePipelineConfigureFnResult {
    if args.kind != PipelineModuleKind::Destination {
        return ModulePipelineConfigureFnResult::ErrorKindNotSupported
    }
    
    set_pipeline_module_configuration(args);
    ModulePipelineConfigureFnResult::Ok
}

#[no_mangle]
extern "C" fn torustiq_module_common_start(handle: ModuleHandle) -> StepStartFnResult {
    let args = match get_pipeline_module_configuration(handle) {
        Some(a) => a,
        None => return StepStartFnResult::ErrorMisc(string_to_cchar(format!("Init args for step '{}' not found", handle)))
    };

    let step_params = match get_params(args.module_handle) {
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
    StepStartFnResult::Ok
}

#[no_mangle]
extern "C" fn torustiq_module_pipeline_process_record(input: Record, _h: ModuleHandle) -> ModulePipelineProcessRecordFnResult {
    let producer = match PRODUCER.lock().unwrap().clone() {
        Some(p) => p,
        None => {
            error!("Cannot send a message to Kafka: producer is offline");
            return ModulePipelineProcessRecordFnResult::Ok
        }
    };
    // TODO:
    // Instead of blocking, process a message using channel
    // Consider moving the torustiq_module_pipeline_process_record function into common module, so all modules are expected
    //   to process records in async mode (using channels)
    let mtd = input.get_metadata_as_hashmap();
    let headers: HashMap<String, String> = mtd
        .iter()
        .filter(|(k, _)| k.starts_with("kafka.headers."))
        .map(|(k, v)| (k.strip_prefix("kafka.headers.").unwrap().to_string(), v.clone()))
        .collect();
    let key = mtd.get("kafka.key").cloned();
    let topic = mtd.get("kafka.topic").unwrap_or(&String::from("test")).clone(); // TODO: handle the missing topic
    if let Err(e) = futures::executor::block_on(producer.produce(&KafkaMessage {
        headers,
        key,
        payload: input.content.to_byte_vec(),
        topic,
    })) {
        print!("Failed to send a message to Kafka: {}", e)
    }
    ModulePipelineProcessRecordFnResult::Ok
}