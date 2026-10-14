#pragma once

#include <cstdint>
#include <atomic>
#include <string>
#include <sstream>
#include <vector>
#include <algorithm>
#include <mutex>

namespace streamforge::observability {

class MetricsRegistry {
 public:
  static MetricsRegistry& instance() {
    static MetricsRegistry registry;
    return registry;
  }

  void record_append(std::uint64_t latency_ns, std::uint64_t records_count, std::uint64_t bytes_count) {
    append_ops_++;
    append_records_ += records_count;
    append_bytes_ += bytes_count;
    std::lock_guard lock(latency_mutex_);
    if (append_latencies_ns_.size() < 10000) {
      append_latencies_ns_.push_back(latency_ns);
    }
  }

  void record_fetch(std::uint64_t latency_ns, std::uint64_t records_count) {
    fetch_ops_++;
    fetch_records_ += records_count;
    std::lock_guard lock(latency_mutex_);
    if (fetch_latencies_ns_.size() < 10000) {
      fetch_latencies_ns_.push_back(latency_ns);
    }
  }

  void record_connection_opened() {
    active_connections_++;
    total_connections_++;
  }

  void record_connection_closed() {
    if (active_connections_ > 0) active_connections_--;
  }

  [[nodiscard]] std::string export_prometheus() const {
    std::ostringstream out;

    out << "# HELP eventstream_append_operations_total Total number of append operations\n"
        << "# TYPE eventstream_append_operations_total counter\n"
        << "eventstream_append_operations_total " << append_ops_.load() << "\n\n";

    out << "# HELP eventstream_append_records_total Total number of records appended\n"
        << "# TYPE eventstream_append_records_total counter\n"
        << "eventstream_append_records_total " << append_records_.load() << "\n\n";

    out << "# HELP eventstream_append_bytes_total Total payload bytes appended\n"
        << "# TYPE eventstream_append_bytes_total counter\n"
        << "eventstream_append_bytes_total " << append_bytes_.load() << "\n\n";

    out << "# HELP eventstream_fetch_operations_total Total number of fetch operations\n"
        << "# TYPE eventstream_fetch_operations_total counter\n"
        << "eventstream_fetch_operations_total " << fetch_ops_.load() << "\n\n";

    out << "# HELP eventstream_fetch_records_total Total number of records fetched\n"
        << "# TYPE eventstream_fetch_records_total counter\n"
        << "eventstream_fetch_records_total " << fetch_records_.load() << "\n\n";

    out << "# HELP eventstream_active_connections Current active TCP client connections\n"
        << "# TYPE eventstream_active_connections gauge\n"
        << "eventstream_active_connections " << active_connections_.load() << "\n\n";

    out << "# HELP eventstream_total_connections_total Cumulative TCP connections accepted\n"
        << "# TYPE eventstream_total_connections_total counter\n"
        << "eventstream_total_connections_total " << total_connections_.load() << "\n\n";

    // Compute latency percentiles
    std::vector<std::uint64_t> app_lats;
    std::vector<std::uint64_t> fetch_lats;
    {
      std::lock_guard lock(latency_mutex_);
      app_lats = append_latencies_ns_;
      fetch_lats = fetch_latencies_ns_;
    }

    auto calc_p = [](std::vector<std::uint64_t>& vals, double p) -> double {
      if (vals.empty()) return 0.0;
      std::sort(vals.begin(), vals.end());
      const std::size_t idx = static_cast<std::size_t>(p * static_cast<double>(vals.size() - 1));
      return static_cast<double>(vals[idx]) / 1'000'000.0; // convert to ms
    };

    out << "# HELP eventstream_append_latency_ms Append latency percentiles in ms\n"
        << "# TYPE eventstream_append_latency_ms summary\n"
        << "eventstream_append_latency_ms{quantile=\"0.50\"} " << calc_p(app_lats, 0.50) << "\n"
        << "eventstream_append_latency_ms{quantile=\"0.95\"} " << calc_p(app_lats, 0.95) << "\n"
        << "eventstream_append_latency_ms{quantile=\"0.99\"} " << calc_p(app_lats, 0.99) << "\n\n";

    out << "# HELP eventstream_fetch_latency_ms Fetch latency percentiles in ms\n"
        << "# TYPE eventstream_fetch_latency_ms summary\n"
        << "eventstream_fetch_latency_ms{quantile=\"0.50\"} " << calc_p(fetch_lats, 0.50) << "\n"
        << "eventstream_fetch_latency_ms{quantile=\"0.95\"} " << calc_p(fetch_lats, 0.95) << "\n"
        << "eventstream_fetch_latency_ms{quantile=\"0.99\"} " << calc_p(fetch_lats, 0.99) << "\n";

    return out.str();
  }

  void reset() {
    append_ops_ = 0;
    append_records_ = 0;
    append_bytes_ = 0;
    fetch_ops_ = 0;
    fetch_records_ = 0;
    active_connections_ = 0;
    total_connections_ = 0;
    std::lock_guard lock(latency_mutex_);
    append_latencies_ns_.clear();
    fetch_latencies_ns_.clear();
  }

 private:
  MetricsRegistry() = default;

  std::atomic_uint64_t append_ops_{0};
  std::atomic_uint64_t append_records_{0};
  std::atomic_uint64_t append_bytes_{0};
  std::atomic_uint64_t fetch_ops_{0};
  std::atomic_uint64_t fetch_records_{0};
  std::atomic_uint64_t active_connections_{0};
  std::atomic_uint64_t total_connections_{0};

  mutable std::mutex latency_mutex_;
  std::vector<std::uint64_t> append_latencies_ns_;
  std::vector<std::uint64_t> fetch_latencies_ns_;
};

}  // namespace streamforge::observability
