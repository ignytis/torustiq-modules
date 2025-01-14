#ifndef _TORUSTIQ_COMMON_H_
#define _TORUSTIQ_COMMON_H_

#include <cstdarg>
#include <cstdint>
#include <cstdlib>
#include <ostream>
#include <new>

namespace torustiq_common {

constexpr static const uint32_t CURRENT_API_VERSION = 1;

enum class ModuleKind {
  /// A pipeline step module. Extracts, transforms, loads the data.
  Step,
  /// An event listener module. Reacts to application events.
  Listener,
};

/// Specifies the position of step in pipeline
enum class PipelineModuleKind {
  /// Source: produces the data itself, no input from other steps is expected.
  Source,
  /// Transformation: gets the data from the previous step (source or transformation)
  /// and sends the processed result to the next step
  Transformation,
  /// Destination: a final point in the pipeline. Receives the data, but doesn't send it
  /// to any further step
  Destination,
};

using Uint = unsigned int;

using ModuleHandle = Uint;

using ConstCharPtr = const char*;

struct ByteBuffer {
  uint8_t *bytes;
  uintptr_t len;
};

/// Record metadata. Each item is a key-value pair + a reference to the next record
struct RecordMetadata {
  ConstCharPtr name;
  ConstCharPtr value;
};

template<typename T>
struct Array {
  T *data;
  Uint len;
};

/// A single piece of data to transmit. Contains the data itself + metadata
struct Record {
  ByteBuffer content;
  Array<RecordMetadata> metadata;
};

/// A result of sending a record to further processing
struct ModulePipelineProcessRecordFnResult {
  enum class Tag {
    /// Processing succeeded. No immediate error occurred
    Ok,
    /// Cannot proces record due to error
    Err,
  };

  struct Err_Body {
    ConstCharPtr _0;
  };

  Tag tag;
  union {
    Err_Body err;
  };
};

/// Module information
struct LibInfo {
  Uint api_version;
  ConstCharPtr id;
  ModuleKind kind;
  ConstCharPtr name;
};

using ModuleTerminationHandlerFn = void(*)(Uint);

/// A callback for received data processed by main app. Arguments are:
/// 1. A record: payload + metadata
/// 2. Step handle to identity the source
using ModuleOnDataReceiveCb = void(*)(Record, ModuleHandle);

/// Arguments passed to init function
struct ModulePipelineConfigureArgs {
  PipelineModuleKind kind;
  ModuleHandle module_handle;
  ModuleTerminationHandlerFn on_step_terminate_cb;
  ModuleOnDataReceiveCb on_data_receive_cb;
};

/// Returns the status of module step configuration
struct ModulePipelineConfigureFnResult {
  enum class Tag {
    /// Configuration succeeded
    Ok,
    /// The provided kind (source, transformation, destination) is not supported by module.
    /// Modules don't necessarily can handle all kinds of steps
    ErrorKindNotSupported,
    /// Module can be used in one step only.
    /// Some modules can have issues with having initialized for multiple steps
    /// Argument is a handle of previously initialized module which caused a conflict
    ErrorMultipleStepsNotSupported,
    /// Other kind of error occurred. More details in text message
    ErrorMisc,
  };

  struct ErrorMultipleStepsNotSupported_Body {
    ModuleHandle _0;
  };

  struct ErrorMisc_Body {
    ConstCharPtr _0;
  };

  Tag tag;
  union {
    ErrorMultipleStepsNotSupported_Body error_multiple_steps_not_supported;
    ErrorMisc_Body error_misc;
  };
};

/// Returns the status of module step start
struct StepStartFnResult {
  enum class Tag {
    /// Started successfully
    Ok,
    /// Other kind of error occurred. More details in text message
    ErrorMisc,
  };

  struct ErrorMisc_Body {
    ConstCharPtr _0;
  };

  Tag tag;
  union {
    ErrorMisc_Body error_misc;
  };
};

using ConstCStrPtr = const int8_t*;

extern "C" {

/// Sets a parameter for step
void torustiq_module_common_set_param(ModuleHandle h, ConstCharPtr k, ConstCharPtr v);

/// Called by main application to trigger the shutdown
void torustiq_module_common_shutdown(ModuleHandle h);

/// Deallocates memory for a record
void torustiq_module_pipeline_free_record(Record r);

/// Deallocates memory for a record
void torustiq_module_common_free_char(ConstCharPtr c);

ModulePipelineProcessRecordFnResult torustiq_module_pipeline_process_record(Record in_record,
                                                                ModuleHandle module_handle);

}  // extern "C"

}  // namespace torustiq_common

#endif  // _TORUSTIQ_COMMON_H_
