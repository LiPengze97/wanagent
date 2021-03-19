#pragma once
#include <derecho/mutils-serialization/SerializationSupport.hpp>
#include <functional>
#include <string.h>
#include <iostream>
#include <string>

namespace wan_agent {

class Blob : public mutils::ByteRepresentable {
public:
    char* bytes;
    std::size_t size;
    bool is_temporary;

    Blob(const char* const b, const decltype(size) s);

    Blob(char* b, const decltype(size) s, bool temporary);

    Blob(const Blob& other);

    Blob(Blob&& other);

    Blob();

    virtual ~Blob();

    Blob& operator=(Blob&& other);

    Blob& operator=(const Blob& other);

    std::size_t to_bytes(char* v) const;

    std::size_t bytes_size() const;

    void post_object(const std::function<void(char const* const, std::size_t)>& f) const;

    void ensure_registered(mutils::DeserializationManager&) {}

    static std::unique_ptr<Blob> from_bytes(mutils::DeserializationManager*, const char* const v);

    mutils::context_ptr<Blob> from_bytes_noalloc(
        mutils::DeserializationManager* ctx,
        const char* const v);

    mutils::context_ptr<Blob> from_bytes_noalloc_const(
        mutils::DeserializationManager* ctx,
        const char* const v);
};

}