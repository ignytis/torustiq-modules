use metrics_exporter_prometheus::PrometheusBuilder;
use torustiq_common::{
    ffi::{
        shared::set_step_configuration,
        types::module::{ModuleInfo, ModuleKind, ModulePipelineStepConfigureArgs, ModuleStepConfigureFnResult, ModuleStepHandle, StepStartFnResult}}, logging::init_logger, CURRENT_API_VERSION
};

const MODULE_INFO: ModuleInfo = ModuleInfo {
    api_version: CURRENT_API_VERSION,
    id: c"prometheus".as_ptr(),
    kind: ModuleKind::EventListener,
    name: c"Prometheus metrics".as_ptr(),
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
extern "C" fn torustiq_module_step_configure(args: ModulePipelineStepConfigureArgs) -> ModuleStepConfigureFnResult {
    set_step_configuration(args);
    ModuleStepConfigureFnResult::Ok
}

#[no_mangle]
extern "C" fn torustiq_module_step_start(handle: ModuleStepHandle) -> StepStartFnResult {
    let builder = PrometheusBuilder::new();
    builder.install().expect("failed to install recorder/exporter");

    StepStartFnResult::Ok
}
