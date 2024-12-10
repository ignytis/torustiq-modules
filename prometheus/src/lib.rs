use metrics_exporter_prometheus::PrometheusBuilder;
use torustiq_common::{
    ffi::{
        shared::set_listener_module_configuration,
        types::module as module_types,  
    },
    logging::init_logger,
    CURRENT_API_VERSION,
};

const MODULE_INFO: module_types::ModuleInfo = module_types::ModuleInfo {
    api_version: CURRENT_API_VERSION,
    id: c"prometheus".as_ptr(),
    kind: module_types::ModuleKind::Listener,
    name: c"Prometheus metrics".as_ptr(),
};

#[no_mangle]
pub extern "C" fn torustiq_module_get_info() -> module_types::ModuleInfo {
    MODULE_INFO
}

#[no_mangle]
extern "C" fn torustiq_module_init() {
    init_logger();
}


#[no_mangle]
extern "C" fn torustiq_module_listener_configure(args: module_types::ModuleListenerConfigureArgs) -> module_types::ModuleListenerConfigureFnResult {
    set_listener_module_configuration(args);
    module_types::ModuleListenerConfigureFnResult::Ok
}

#[no_mangle]
extern "C" fn torustiq_module_common_start(_handle: module_types::ModuleHandle) -> module_types::StepStartFnResult {
    let builder = PrometheusBuilder::new();
    builder.install().expect("failed to install recorder/exporter");

    module_types::StepStartFnResult::Ok
}
