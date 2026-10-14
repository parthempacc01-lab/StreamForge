#pragma once
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>
namespace streamforge::benchmark {
struct Options { std::string data_directory; std::size_t messages{10000}; std::size_t payload_bytes{256}; std::size_t batch_size{1}; std::size_t warmup_batches{10}; std::size_t repetitions{5}; std::size_t clients{1}; std::size_t partitions{1}; bool profile{false}; std::string mode{"appended"}; };
struct Summary { double p50_ms; double p95_ms; double p99_ms; };
[[nodiscard]] Options parse_options(int argc, char** argv);
[[nodiscard]] double percentile(std::vector<double> samples, double fraction);
[[nodiscard]] Summary summarize(std::vector<double> samples);
[[nodiscard]] std::uint32_t target_partition(std::size_t client_index, std::size_t partitions);
[[nodiscard]] std::vector<std::size_t> messages_per_partition(std::size_t messages,
                                                               std::size_t clients,
                                                               std::size_t partitions);
}
