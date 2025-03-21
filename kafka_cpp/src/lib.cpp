#include "lib.hpp"

const char *MODULE_ID = "kafka_cpp";
const char *MODULE_NAME = "C++ implementation of Kafka";
const LibInfo MODULE_INFO = {
    .api_version = CURRENT_API_VERSION,
    .id = MODULE_ID,
    .kind = ModuleKind::Pipeline,
    .name = MODULE_NAME,
};

map<ModuleHandle, ModulePipelineConfigureArgs> ARGS;
map<ModuleHandle, map<string, string>> STEP_PARAMS;

Producer *PRODUCER = nullptr;

extern "C" LibInfo torustiq_module_get_info()
{
    return MODULE_INFO;
}

extern "C" void torustiq_module_init()
{
    // no action
}

extern "C" ModulePipelineConfigureFnResult torustiq_module_pipeline_configure(ModulePipelineConfigureArgs args)
{
    ModulePipelineConfigureFnResult result;
    
    if (args.kind != PipelineModuleKind::Destination)
    {
        return {
            .tag = ModulePipelineConfigureFnResult::Tag::ErrorKindNotSupported,
        };
    }

    ARGS[args.module_handle] = args;

    return {
        .tag = ModulePipelineConfigureFnResult::Tag::Ok
    };
}

extern "C" void torustiq_module_common_set_param(ModuleHandle h, ConstCharPtr k, ConstCharPtr v)
{
    if (!maps::key_exists(h, STEP_PARAMS))
    {
        STEP_PARAMS[h] = {};
    }

    STEP_PARAMS[h][string(k)] = string(v);
}

extern "C" void torustiq_module_common_shutdown(ModuleHandle h)
{
    if (!maps::key_exists(h, ARGS))
    {
        return;
    }
    ARGS[h].on_step_terminate_cb(h);
}

extern "C" void torustiq_module_common_free_char(const char* c) {
    delete c;
}

extern "C" void torustiq_module_pipeline_free_record(Record r) {
    delete r.content.bytes;
    delete r.metadata.data;
}

extern "C" StepStartFnResult torustiq_module_common_start(ModuleHandle h)
{
    if (!maps::key_exists(h, ARGS))
    {
        return {
            .tag = StepStartFnResult::Tag::ErrorMisc,
            .error_misc = {
                // TODO:
                // Free result here and below
                // Is free result needed in Rust code too?
                ._0 = (new string(string("Init args for step '") + to_string(h) + string("' not found")))->c_str(),
            },
        };
    }
    if (!maps::key_exists(h, STEP_PARAMS))
    {
        return {
            .tag = StepStartFnResult::Tag::ErrorMisc,
            .error_misc = {
                ._0 = (new string(string("Step params for step '") + to_string(h) + string("' not found")))->c_str(),
            },
        };
    }

    ModulePipelineConfigureArgs args = ARGS[h];
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
    optional<string> error = PRODUCER->start();
    StepStartFnResult res;
    if (nullopt == error)
    {
        res  = {
            .tag = StepStartFnResult::Tag::Ok,
        };
    } else {
        string *err = new string(error.value());
        res = {
            .tag = StepStartFnResult::Tag::ErrorMisc,
            .error_misc = {
                ._0 = err->c_str(),
            },
        };
    }

    return res;
}

extern "C" ModulePipelineProcessRecordFnResult torustiq_module_pipeline_process_record(Record in, ModuleHandle h)
{
    map<string, string> metadata;
    RecordMetadata *mtd_raw_last = in.metadata.data + in.metadata.len;
    for (RecordMetadata *mtd_raw = in.metadata.data; mtd_raw < mtd_raw_last; mtd_raw++)
    {
        metadata[string(mtd_raw->name)] = string(mtd_raw->value);
    }
    // Extract headers
    map<string, string> headers;
    for (pair<const string, string> item: metadata)
    {
        if (!strings::begins_with(item.first, "kafka.headers."))
        {
            continue;
        }

        string k = strings::strip_prefix(item.first, "kafka.headers.");
        headers[k] = item.second;
    }

    optional<string> key = nullopt;
    if (maps::key_exists("kafka.key", metadata))
    {
        key = metadata["kafka.key"];
    }

    
    if (!maps::key_exists("kafka.topic", metadata))
    {
        return {
            .tag = ModulePipelineProcessRecordFnResult::Tag::Err,
            .err = (new string("Missing the topic name in metadata"))->c_str(),
        };
    }
    string topic = metadata["kafka.topic"];

    optional<string> err = PRODUCER->produce(topic, &key, &headers, &in.content);
    if (err.has_value())
    {
        return {
            .tag = ModulePipelineProcessRecordFnResult::Tag::Err,
            .err = (new string(err.value()))->c_str(),
        };
    }


    return {
        .tag = ModulePipelineProcessRecordFnResult::Tag::Ok,
    };
}