#include "streamforge/protocol/frame_codec.hpp"

#include <cstddef>
#include <cstdint>
#include <span>

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
  const auto* bytes = reinterpret_cast<const std::byte*>(data);
  streamforge::protocol::FrameDecoder decoder;
  (void)decoder.feed(std::span{bytes, size});
  while (decoder.has_frame()) {
    (void)decoder.take_frame();
  }
  return 0;
}
