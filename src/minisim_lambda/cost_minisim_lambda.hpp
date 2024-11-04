#ifndef COSTMINISIMLAMBDA_HPP
#define COSTMINISIMLAMBDA_HPP

#include <string>
#include <aws/lambda-runtime/runtime.h>

static aws::lambda_runtime::invocation_response cost_minisim_handler(aws::lambda_runtime::invocation_request const& request);

#endif // COSTMINISIMLAMBDA_HPP