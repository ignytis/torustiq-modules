use actix_web::{post,  web::{Bytes, Data}, App, HttpServer, Responder};

use torustiq_common::ffi::types::{
    buffer::ByteBuffer,
    functions::ModuleOnDataReceivedFn,
    module::{ModuleInitStepArgs, ModuleStepHandle, Record}};

pub fn run_server(args: ModuleInitStepArgs) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    match rt.block_on(async { do_spawn_server(args).await }) {
        Ok(_) => {},
        Err(e) => println!("Cannot create the runtime for HTTP server: {:?}", e),
    }
}

struct HttpAppData {
    torustiq_step_handle: ModuleStepHandle,
    on_request_callback: ModuleOnDataReceivedFn,
}

#[post("/")]
async fn post_request_handler(payload: Bytes, data: Data<HttpAppData>) -> impl Responder {
    let mut dst: Vec<u8> = Vec::with_capacity(payload.len());
    unsafe { std::ptr::copy(payload.as_ptr(), dst.as_mut_ptr(), payload.len()) };
    let bytes = dst.as_mut_ptr();
    std::mem::forget(dst);

    let record = Record {
        content: ByteBuffer{
            bytes,
            len: payload.len(),
        },
    };
    (data.on_request_callback)(record, data.torustiq_step_handle);
    format!("")
}

async fn do_spawn_server(args: ModuleInitStepArgs) -> std::io::Result<()> {
    HttpServer::new(move|| {
        // This termination handler sends signal to the main app
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.unwrap();
            (args.termination_handler)(args.step_handle)
        });
        App::new()
            .app_data(Data::new(HttpAppData {
                on_request_callback: args.on_data_received_fn,
                torustiq_step_handle: args.step_handle,
            }))
            .service(post_request_handler)
    })
    .bind(("127.0.0.1", 8080)).unwrap()
    .run().await
}