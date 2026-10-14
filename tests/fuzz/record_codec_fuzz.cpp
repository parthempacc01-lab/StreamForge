#include "streamforge/storage/record_codec.hpp"

#include <cstddef>
#include <cstdint>
#include <span>

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
  const auto* bytes = reinterpret_cast<const std::byte*>(data);
  (void)streamforge::storage::scan_records(std::span{bytes, size}, 0);
  return 0;
}
