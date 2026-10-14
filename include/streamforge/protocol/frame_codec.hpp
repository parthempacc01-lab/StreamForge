#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <vector>

namespace streamforge::protocol {

inline constexpr std::uint16_t kProtocolMagic = 0x5346;  // "SF"
inline constexpr std::uint8_t kProtocolVersion = 1;
inline constexpr std::size_t kFramePrefixSize = 4;
inline constexpr std::size_t kFrameHeaderSize = 16;
inline constexpr std::size_t kMinimumFrameBodySize = kFrameHeaderSize;
inline constexpr std::size_t kDefaultMaximumFrameBodySize = 4U * 1024U * 1024U;

enum class Operation : std::uint8_t {
  kPing = 1,
  kCreateTopic = 2,
  kDescribeTopic = 3,
  kProduceBatch = 4,
  kFetch = 5,
  kCommitOffsets = 6,
  kJoinGroup = 7,
  kHeartbeat = 8,
  kLeaveGroup = 9,
  kGetMetrics = 10,
};

enum class Status : std::uint16_t {
  kOk = 0,
  kMalformedFrame = 1,
  kUnsupportedVersion = 2,
  kUnknownOperation = 3,
  kOversized = 4,
  kInternal = 255,
};

struct FrameHeader {
  std::uint8_t version{kProtocolVersion};
  Operation operation{Operation::kPing};
  std::uint16_t flags{0};
  std::uint64_t request_id{0};
  Status status{Status::kOk};
};

struct Frame {
  FrameHeader header;
  std::vector<std::byte> payload;
};

enum class DecodeErrorCode {
  kInvalidLength,
  kOversizedFrame,
  kInvalidMagic,
  kUnsupportedVersion,
  kUnknownOperation,
};

struct DecodeError {
  DecodeErrorCode code;
  std::string message;
};

class FrameDecoder {
 public:
  explicit FrameDecoder(std::size_t maximum_frame_body_size =
                            kDefaultMaximumFrameBodySize);

  // Adds arbitrary bytes from a TCP stream. On error, the decoder becomes
  // terminal; the caller must close the connection and construct a new one.
  [[nodiscard]] bool feed(std::span<const std::byte> bytes);
  [[nodiscard]] bool has_frame() const noexcept;
  [[nodiscard]] Frame take_frame();
  [[nodiscard]] const DecodeError* error() const noexcept;

 private:
  [[nodiscard]] bool decode_available_frames();
  [[nodiscard]] bool set_error(DecodeErrorCode code, std::string message);

  std::size_t maximum_frame_body_size_;
  std::vector<std::byte> pending_;
  std::vector<Frame> ready_;
  DecodeError error_{};
  bool has_error_{false};
};

[[nodiscard]] std::vector<std::byte> encode_frame(const Frame& frame);

}  // namespace streamforge::protocol
