#include "interceptor.h"
#include <iostream>
#include "TextMapCarrier.h"
#include "tracing/TracerUtil.h"

using namespace grpc;
using namespace std;

SpanInterceptor::SpanInterceptor(experimental::ServerRpcInfo* info, std::shared_ptr<opentracing::Tracer> tracer)
: info_(info), tracer_(tracer) {
}

void SpanInterceptor::Intercept(experimental::InterceptorBatchMethods* methods) {
    if (methods->QueryInterceptionHookPoint(
                experimental::InterceptionHookPoints::POST_RECV_INITIAL_METADATA)) {
        cout << "experimental::InterceptionHookPoints::POST_RECV_INITIAL_METADATA ..." << endl;
        std::unordered_map<std::string, std::string> text_map;
        auto* map = methods->GetRecvInitialMetadata();
        /* for (auto kv : *map) { */
        /*     cout << string(kv.first.data(), kv.first.length()) << " : " */
        /*         << string(kv.second.data(), kv.second.length()) << endl; */
        /* } */

        auto context_kv = map->find(TracerUtil::GetTraceContextHeaderName());
        if (context_kv != map->end()) {
            text_map[string(context_kv->first.data(), context_kv->first.length())] =
                string(context_kv->second.data(), context_kv->second.length());
        }

        TextMapCarrier carrier{text_map};
        auto span_maybe = tracer_->Extract(carrier);
        span_ = tracer_->StartSpan(info_->method(), {opentracing::ChildOf(span_maybe->get())});

    } else if (methods->QueryInterceptionHookPoint(
                  experimental::InterceptionHookPoints::PRE_SEND_MESSAGE)) {
        cout << "experimental::InterceptionHookPoints::PRE_SEND_MESSAGE ..." << endl;

        span_->Finish();
    }

    methods->Proceed();
}

experimental::Interceptor* SpanInterceptorFactory::CreateServerInterceptor(
        grpc::experimental::ServerRpcInfo* info) {
    return new SpanInterceptor(info, tracer_);
}
