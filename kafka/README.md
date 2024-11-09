# Kafka module

__This module is deprecated.__ There are issues with compilation periodically occur with this module. Having wrapper dependencies (for `librdkafka` in this case) in the project causes troubles with indirect deps, so I decided to implement this library in C++ instead - see `kafka_cpp`. 

The last known issue is:
```
.cargo/git/checkouts/rust-rdkafka-efec30e1ac0c60a9/f5782a4/rdkafka-sys/librdkafka/src/rdkafka_conf.h:39:10: fatal error: openssl/engine.h: No such file or directory
     39 | #include <openssl/engine.h>
        |          ^~~~~~~~~~~~~~~~~~
  compilation terminated.
```