#include "streamforge/protocol/frame_codec.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

#include <gtest/gtest.h>

namespace streamforge::protocol {
namespace {

Frame ping_frame(std::uint64_t request_id) {
  return Frame{FrameHeader{.operation = Operation::kPing, .request_id = request_id},
               {std::byte{0xA1}, std::byte{0xB2}}};
}

TEST(FrameCodec, DecodesAFrameSplitAcrossArbitraryReads) {
  const auto encoded = encode_frame(ping_frame(42));
  FrameDecoder decoder;

  EXPECT_TRUE(decoder.feed(std::span{encoded}.first(3)));
  EXPECT_FALSE(decoder.has_frame());
  EXPECT_TRUE(decoder.feed(std::span{encoded}.subspan(3, 7)));
  EXPECT_FALSE(decoder.has_frame());
  EXPECT_TRUE(decoder.feed(std::span{encoded}.subspan(10)));

  ASSERT_TRUE(decoder.has_frame());
  const auto decoded = decoder.take_frame();
  EXPECT_EQ(decoded.header.request_id, 42U);
  EXPECT_EQ(decoded.header.operation, Operation::kPing);
  EXPECT_EQ(decoded.payload.size(), 2U);
}

TEST(FrameCodec, DecodesCoalescedFramesInOrder) {
  const auto first = encode_frame(ping_frame(1));
  const auto second = encode_frame(ping_frame(2));
  std::vector<std::byte> bytes = first;
  bytes.insert(bytes.end(), second.begin(), second.end());

  FrameDecoder decoder;
  ASSERT_TRUE(decoder.feed(bytes));
  ASSERT_TRUE(decoder.has_frame());
  EXPECT_EQ(decoder.take_frame().header.request_id, 1U);
  ASSERT_TRUE(decoder.has_frame());
  EXPECT_EQ(decoder.take_frame().header.request_id, 2U);
  EXPECT_FALSE(decoder.has_frame());
}

TEST(FrameCodec, RejectsTooSmallLengthBeforeWaitingForBody) {
  const std::array bytes{std::byte{0}, std::byte{0}, std::byte{0}, std::byte{15}};
  FrameDecoder decoder;

  EXPECT_FALSE(decoder.feed(bytes));
  ASSERT_NE(decoder.error(), nullptr);
  EXPECT_EQ(decoder.error()->code, DecodeErrorCode::kInvalidLength);
}

TEST(FrameCodec, RejectsOversizedLengthBeforeAllocatingBody) {
  const std::array bytes{std::byte{0}, std::byte{0}, std::byte{0}, std::byte{32}};
  FrameDecoder decoder(16);

  EXPECT_FALSE(decoder.feed(bytes));
  ASSERT_NE(decoder.error(), nullptr);
  EXPECT_EQ(decoder.error()->code, DecodeErrorCode::kOversizedFrame);
}

TEST(FrameCodec, RejectsUnknownOperation) {
  auto encoded = encode_frame(ping_frame(1));
  encoded[7] = std::byte{0xFF};
  FrameDecoder decoder;

  EXPECT_FALSE(decoder.feed(encoded));
  ASSERT_NE(decoder.error(), nullptr);
  EXPECT_EQ(decoder.error()->code, DecodeErrorCode::kUnknownOperation);
}

TEST(FrameCodec, RejectsInvalidMagic) {
  auto encoded = encode_frame(ping_frame(1));
  encoded[4] = std::byte{0x00};
  FrameDecoder decoder;

  EXPECT_FALSE(decoder.feed(encoded));
  ASSERT_NE(decoder.error(), nullptr);
  EXPECT_EQ(decoder.error()->code, DecodeErrorCode::kInvalidMagic);
}

TEST(FrameCodec, RejectsUnsupportedVersion) {
  auto encoded = encode_frame(ping_frame(1));
  encoded[6] = std::byte{0x02};
  FrameDecoder decoder;

  EXPECT_FALSE(decoder.feed(encoded));
  ASSERT_NE(decoder.error(), nullptr);
  EXPECT_EQ(decoder.error()->code, DecodeErrorCode::kUnsupportedVersion);
}

}  // namespace
}  // namespace streamforge::protocol
