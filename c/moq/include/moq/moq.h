/* The moq C package: the C API generated from moq-ffi, plus the shutdown it cannot generate. */
#ifndef MOQ_MOQ_H
#define MOQ_MOQ_H

#include <moq/ffi/moq.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Stops the moq runtime workers, then the dispatcher, before the process or module goes away.
 * Pending calls never call back. Call it once, from a thread that is not running a callback.
 */
void moq_shutdown(void);

#ifdef __cplusplus
}
#endif

#endif
