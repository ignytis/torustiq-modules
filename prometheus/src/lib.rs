mod metrics;

use std::{
    collections::HashMap,
    net::SocketAddrV4,
    thread,
};

use log::debug;
use metrics::{init, thread_refresh};
use metrics_exporter_prometheus::PrometheusBuilder;

use torustiq_common::{
    ffi::{
        shared::{get_params, set_listener_module_configuration},
        types::module as module_types, utils::strings::string_to_cchar,
    },
    logging::init_logger,
    CURRENT_API_VERSION,
};

const MODULE_INFO: module_types::LibInfo = module_types::LibInfo {
    api_version: CURRENT_API_VERSION,
    id: c"prometheus".as_ptr(),
    kind: module_types::ModuleKind::Listener,
    name: c"Prometheus metrics".as_ptr(),
};

const DEFAULT_LISTEN_HOST: &str = "0.0.0.0";
const DEFAULT_LISTEN_PORT: &str = "9000";



#[no_mangle]
pub extern "C" fn torustiq_module_get_info() -> module_types::LibInfo {
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
extern "C" fn torustiq_module_common_start(handle: module_types::ModuleHandle) -> module_types::StepStartFnResult {
    debug!("Starting...");
    let params = get_params(handle).unwrap_or(HashMap::new());
    let host = params.get("listen.host").map(String::clone).unwrap_or(DEFAULT_LISTEN_HOST.into());
    let port = params.get("port").map(String::clone).unwrap_or(DEFAULT_LISTEN_PORT.into());
    let sock_addr_str = format!("{}:{}", host, port);
    let sock_addr: SocketAddrV4 = match sock_addr_str.parse() {
        Ok(a) => a,
        Err(e) => return module_types::StepStartFnResult::ErrorMisc(
            string_to_cchar(format!("Failed to format an endpoint address from host '{}' and port '{}': {}", host, port, e)))
    };
    let builder = PrometheusBuilder::new()
        .with_http_listener(sock_addr);
    if let Err(e) = builder.install() {
        return module_types::StepStartFnResult::ErrorMisc(
            string_to_cchar(format!("Cannot listen a socket on host '{}' and port '{}': {}", host, port, e)));
    }
    debug!("Started Prometheus metrics server on '{}'.", sock_addr_str);

    if let Err(e) = init(handle) {
        return module_types::StepStartFnResult::ErrorMisc(
            string_to_cchar(format!("Failed to init metrics for handle '{}': {}", handle, e)));
    }
    thread::spawn(thread_refresh);

    module_types::StepStartFnResult::Ok
}

#[no_mangle]
extern "C" fn torustiq_module_listener_record_rcv(handle: module_types::ModuleHandle, record: *const module_types::Record)  {
    metrics::inc_stats_msg_in(handle, record);
}

#[no_mangle]
extern "C" fn torustiq_module_listener_record_send_failure(handle: module_types::ModuleHandle, record: *const module_types::Record)  {
    metrics::inc_stats_errors_num(handle, record);
}

#[no_mangle]
extern "C" fn torustiq_module_listener_record_send_success(handle: module_types::ModuleHandle, record: *const module_types::Record)  {
    metrics::inc_stats_msg_out  (handle, record);
}