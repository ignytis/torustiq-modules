use actix_web::{post, web::{Bytes, Data}, App, HttpRequest, HttpServer, Responder};
use log::debug;

use torustiq_common::ffi::{types::{
    buffer::ByteBuffer, collections::Array, functions::ModuleOnDataReceivedFn, module::{ModuleStepHandle, ModulePipelineStepConfigureArgs, Record, RecordMetadata}}, utils::strings::str_to_cchar};

pub fn run_server(args: ModulePipelineStepConfigureArgs, host: String, port: u16) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    match rt.block_on(async { do_spawn_server(args, host, port).await }) {
        Ok(_) => {},
        Err(e) => println!("Cannot create the runtime for HTTP server: {:?}", e),
    }
}

struct HttpAppData {
    torustiq_step_handle: ModuleStepHandle,
    on_request_callback: ModuleOnDataReceivedFn,
}

#[post("/")]
async fn post_request_handler(payload: Bytes, req: HttpRequest, data: Data<HttpAppData>) -> impl Responder {
    let mut dst: Vec<u8> = Vec::with_capacity(payload.len());
    unsafe { std::ptr::copy(payload.as_ptr(), dst.as_mut_ptr(), payload.len()) };
    let bytes = dst.as_mut_ptr();
    std::mem::forget(dst);

    let mut metadata: Vec<RecordMetadata> = Vec::with_capacity(req.headers().len());
    for (k, v) in req.headers() {
        metadata.push(RecordMetadata {
            name: str_to_cchar(k.as_str()),
            value: str_to_cchar(v.to_str().unwrap()),
        });
    }

    let record = Record {
        content: ByteBuffer{
            bytes,
            len: payload.len(),
        },
        metadata: Array::from_vec(metadata),
    };
    (data.on_request_callback)(record, data.torustiq_step_handle);
    format!("")
}

async fn do_spawn_server(args: ModulePipelineStepConfigureArgs, host: String, port: u16) -> std::io::Result<()> {
    debug!("Starting server at {}:{}", &host, port);
    HttpServer::new(move|| {
        // This termination handler sends signal to the main app
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.unwrap();
            (args.on_step_terminate_cb)(args.step_handle)
        });
        App::new()
            .app_data(Data::new(HttpAppData {
                on_request_callback: args.on_data_received_fn,
                torustiq_step_handle: args.step_handle,
            }))
            .service(post_request_handler)
    })
    .bind((host, port)).unwrap()
    .run().await
}