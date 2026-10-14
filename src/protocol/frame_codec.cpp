#include "streamforge/protocol/frame_codec.hpp"

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <utility>

namespace streamforge::protocol {
namespace {

void append_u16(std::vector<std::byte>& output, std::uint16_t value) {
  output.push_back(static_cast<std::byte>((value >> 8U) & 0xFFU));
  output.push_back(static_cast<std::byte>(value & 0xFFU));
}

void append_u32(std::vector<std::byte>& output, std::uint32_t value) {
  for (int shift = 24; shift >= 0; shift -= 8) {
    output.push_back(static_cast<std::byte>((value >> shift) & 0xFFU));
  }
}

void append_u64(std::vector<std::byte>& output, std::uint64_t value) {
  for (int shift = 56; shift >= 0; shift -= 8) {
    output.push_back(static_cast<std::byte>((value >> shift) & 0xFFU));
  }
}

std::uint16_t read_u16(std::span<const std::byte> input, std::size_t offset) {
  return static_cast<std::uint16_t>(
      (static_cast<std::uint32_t>(input[offset]) << 8U) |
      static_cast<std::uint32_t>(input[offset + 1]));
}

std::uint32_t read_u32(std::span<const std::byte> input, std::size_t offset) {
  std::uint32_t value = 0;
  for (std::size_t index = 0; index < 4; ++index) {
    value = (value << 8U) | static_cast<std::uint32_t>(input[offset + index]);
  }
  return value;
}

std::uint64_t read_u64(std::span<const std::byte> input, std::size_t offset) {
  std::uint64_t value = 0;
  for (std::size_t index = 0; index < 8; ++index) {
    value = (value << 8U) | static_cast<std::uint64_t>(input[offset + index]);
  }
  return value;
}

bool is_known_operation(std::uint8_t value) {
  return value >= static_cast<std::uint8_t>(Operation::kPing) &&
         value <= static_cast<std::uint8_t>(Operation::kGetMetrics);
}

}  // namespace

FrameDecoder::FrameDecoder(std::size_t maximum_frame_body_size)
    : maximum_frame_body_size_(maximum_frame_body_size) {
  if (maximum_frame_body_size_ < kMinimumFrameBodySize) {
    throw std::invalid_argument("maximum frame size is smaller than the header");
  }
}

bool FrameDecoder::feed(std::span<const std::byte> bytes) {
  if (has_error_) {
    return false;
  }

  // Decode incrementally so an untrusted caller cannot make this buffer grow
  // without first passing the prefix length check. The network layer will also
  // impose its own bounded read buffer.
  for (const auto byte : bytes) {
    pending_.push_back(byte);
    if (!decode_available_frames()) {
      return false;
    }
  }
  return true;
}

bool FrameDecoder::has_frame() const noexcept { return !ready_.empty(); }

Frame FrameDecoder::take_frame() {
  if (ready_.empty()) {
    throw std::logic_error("no decoded frame is available");
  }
  Frame frame = std::move(ready_.front());
  ready_.erase(ready_.begin());
  return frame;
}

const DecodeError* FrameDecoder::error() const noexcept {
  return has_error_ ? &error_ : nullptr;
}

bool FrameDecoder::decode_available_frames() {
  while (pending_.size() >= kFramePrefixSize) {
    const auto body_size = read_u32(pending_, 0);
    if (body_size < kMinimumFrameBodySize) {
      return set_error(DecodeErrorCode::kInvalidLength,
                       "frame body is smaller than the fixed header");
    }
    if (body_size > maximum_frame_body_size_) {
      return set_error(DecodeErrorCode::kOversizedFrame,
                       "frame body exceeds the configured maximum");
    }

    const auto total_size = kFramePrefixSize + static_cast<std::size_t>(body_size);
    if (pending_.size() < total_size) {
      return true;
    }

    const std::span<const std::byte> body{pending_.data() + kFramePrefixSize,
                                          static_cast<std::size_t>(body_size)};
    if (read_u16(body, 0) != kProtocolMagic) {
      return set_error(DecodeErrorCode::kInvalidMagic, "invalid protocol magic");
    }
    if (static_cast<std::uint8_t>(body[2]) != kProtocolVersion) {
      return set_error(DecodeErrorCode::kUnsupportedVersion,
                       "unsupported protocol version");
    }
    const auto operation = static_cast<std::uint8_t>(body[3]);
    if (!is_known_operation(operation)) {
      return set_error(DecodeErrorCode::kUnknownOperation, "unknown operation");
    }

    Frame frame;
    frame.header.version = static_cast<std::uint8_t>(body[2]);
    frame.header.operation = static_cast<Operation>(operation);
    frame.header.flags = read_u16(body, 4);
    frame.header.request_id = read_u64(body, 6);
    frame.header.status = static_cast<Status>(read_u16(body, 14));
    frame.payload.assign(body.begin() + static_cast<std::ptrdiff_t>(kFrameHeaderSize),
                         body.end());
    ready_.push_back(std::move(frame));
    pending_.erase(pending_.begin(),
                   pending_.begin() + static_cast<std::ptrdiff_t>(total_size));
  }
  return true;
}

bool FrameDecoder::set_error(DecodeErrorCode code, std::string message) {
  error_ = DecodeError{code, std::move(message)};
  has_error_ = true;
  return false;
}

std::vector<std::byte> encode_frame(const Frame& frame) {
  const auto body_size = kFrameHeaderSize + frame.payload.size();
  if (body_size > std::numeric_limits<std::uint32_t>::max()) {
    throw std::length_error("frame body exceeds protocol length field");
  }

  std::vector<std::byte> encoded;
  encoded.reserve(kFramePrefixSize + body_size);
  append_u32(encoded, static_cast<std::uint32_t>(body_size));
  append_u16(encoded, kProtocolMagic);
  encoded.push_back(static_cast<std::byte>(frame.header.version));
  encoded.push_back(static_cast<std::byte>(frame.header.operation));
  append_u16(encoded, frame.header.flags);
  append_u64(encoded, frame.header.request_id);
  append_u16(encoded, static_cast<std::uint16_t>(frame.header.status));
  encoded.insert(encoded.end(), frame.payload.begin(), frame.payload.end());
  return encoded;
}

}  // namespace streamforge::protocol
