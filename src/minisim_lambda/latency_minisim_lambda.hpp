#ifndef LATENCY_MINISIMLAMBDA_HPP
#define LATENCY_MINISIMLAMBDA_HPP

#include <string>
#include <aws/lambda-runtime/runtime.h>

static aws::lambda_runtime::invocation_response latency_minisim_handler(aws::lambda_runtime::invocation_request const& request);

#endif // LATENCY_MINISIMLAMBDA_HPP