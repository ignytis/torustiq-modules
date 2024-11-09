#include <map>
#include <string>
#include <cstring>

#include "../../torustiq_common_typedefs.hpp"
#include "producer.hpp"

const char *MODULE_ID = "kafka_cpp";
const char *MODULE_NAME = "C++ implementation of Kafka";

using namespace std;
using namespace torustiq_common;
using namespace torustiq_kafka_cpp;

map<ModuleStepHandle, ModuleStepConfigureArgs> ARGS;
map<ModuleStepHandle, map<string, string>> STEP_PARAMS;

Producer *PRODUCER = nullptr;

extern "C" ModuleInfo torustiq_module_get_info()
{
    ModuleInfo r = {
        .id = MODULE_ID,
        .name = MODULE_NAME,
    };
    return r;
}

extern "C" void torustiq_module_init()
{
    // no action
}

extern "C" ModuleStepConfigureFnResult torustiq_module_step_configure(ModuleStepConfigureArgs args)
{
    ModuleStepConfigureFnResult result;
    
    if (args.kind != PipelineStepKind::Destination)
    {
        return {
            .tag = ModuleStepConfigureFnResult::Tag::ErrorKindNotSupported,
        };
    }

    ARGS[args.step_handle] = args;

    return {
        .tag = ModuleStepConfigureFnResult::Tag::Ok
    };
}

extern "C" void torustiq_module_step_set_param(ModuleStepHandle h, ConstCharPtr k, ConstCharPtr v)
{
    if (STEP_PARAMS.find(h) == STEP_PARAMS.end())
    {
        STEP_PARAMS[h] = {};
    }

    STEP_PARAMS[h][string(k)] = string(v);
}

extern "C" void torustiq_module_step_shutdown(ModuleStepHandle h)
{
    if (ARGS.find(h) == ARGS.end())
    {
        return;
    }
    ARGS[h].on_step_terminate_cb(h);
}

extern "C" void torustiq_module_free_record(Record r) {
    delete r.content.bytes;
    delete r.metadata.data;
}

extern "C" ModuleStepStartFnResult torustiq_module_step_start(ModuleStepHandle h)
{
    if (ARGS.find(h) == ARGS.end())
    {
        return {
            .tag = ModuleStepStartFnResult::Tag::ErrorMisc,
            .error_misc = {
                ._0 = (string("Init args for step '") + to_string(h) + string("' not found")).c_str(),
            },
        };
    }
    if (STEP_PARAMS.find(h) == STEP_PARAMS.end())
    {
        return {
            .tag = ModuleStepStartFnResult::Tag::ErrorMisc,
            .error_misc = {
                ._0 = (string("Step params for step '") + to_string(h) + string("' not found")).c_str(),
            },
        };
    }

    ModuleStepConfigureArgs args = ARGS[h];
    map<string, string> step_params = STEP_PARAMS[h];
    map<string, string> driver_params;

    map<string, string>::iterator it;
    for(it = step_params.begin(); it != step_params.end(); it++)
    {
        string k = it->first;
        string v = it->second;
        size_t pos = k.rfind("driver.", 0);
        if (pos != 0)
        {
            continue;
        }
        k = k.substr(strlen("driver."));
        driver_params[k] = v;
    }

    PRODUCER = new Producer(driver_params);
    PRODUCER->start();

    return {
        .tag = ModuleStepStartFnResult::Tag::Ok,
    };
}

extern "C" ModuleProcessRecordFnResult torustiq_module_process_record(Record in, ModuleStepHandle h)
{
    PRODUCER->produce(&in.content);

    return {
        .tag = ModuleProcessRecordFnResult::Tag::Ok
    };

    

    // let producer = match PRODUCER.lock().unwrap().clone() {
    //     Some(p) => p,
    //     None => {
    //         error!("Cannot send a message to Kafka: producer is offline");
    //         return ModuleProcessRecordFnResult::Ok
    //     }
    // };
    // // TODO:
    // // Instead of blocking, process a message using channel
    // // Consider moving the torustiq_module_process_record function into common module, so all modules are expected
    // //   to process records in async mode (using channels)
    // let mtd = input.get_metadata_as_hashmap();
    // let headers: HashMap<String, String> = mtd
    //     .iter()
    //     .filter(|(k, _)| k.starts_with("kafka.headers."))
    //     .map(|(k, v)| (k.strip_prefix("kafka.headers.").unwrap().to_string(), v.clone()))
    //     .collect();
    // let key = mtd.get("kafka.key").cloned();
    // let topic = mtd.get("kafka.topic").unwrap_or(&String::from("test")).clone(); // TODO: handle the missing topic
    // match futures::executor::block_on(producer.produce(&KafkaMessage {
    //     headers,
    //     key,
    //     payload: input.content.to_byte_vec(),
    //     topic,
    // })) {
    //     Err(e) => print!("Failed to send a message to Kafka: {}", e),
    //     _ => {},
    // }
    // ModuleProcessRecordFnResult::Ok
}