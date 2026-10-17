#include "streamforge/storage/durability.hpp"

#include <stdexcept>
#include <system_error>

#if defined(_WIN32)
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#elif defined(__unix__) || defined(__APPLE__)
#include <fcntl.h>
#include <unistd.h>
#endif

namespace streamforge::storage {

void sync_file(const std::filesystem::path& path) {
#if defined(_WIN32)
  HANDLE handle = ::CreateFileW(path.c_str(), GENERIC_READ | GENERIC_WRITE,
                                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                                nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
  if (handle == INVALID_HANDLE_VALUE) {
    handle = ::CreateFileW(path.c_str(), GENERIC_READ,
                           FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                           nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
  }
  if (handle == INVALID_HANDLE_VALUE) {
    throw std::runtime_error("cannot open file for synchronization: " + path.string());
  }
  const BOOL ok = ::FlushFileBuffers(handle);
  ::CloseHandle(handle);
  if (!ok) {
    throw std::runtime_error("FlushFileBuffers failed for file: " + path.string());
  }
#elif defined(__unix__) || defined(__APPLE__)
  const int descriptor = ::open(path.c_str(), O_RDONLY);
  if (descriptor < 0) {
    throw std::runtime_error("cannot open file for synchronization: " + path.string());
  }
  const int result = ::fsync(descriptor);
  const int close_result = ::close(descriptor);
  if (result != 0 || close_result != 0) {
    throw std::runtime_error("fsync failed for file: " + path.string());
  }
#else
  throw std::runtime_error("synchronization not supported on this platform");
#endif
}

void sync_directory(const std::filesystem::path& directory_path) {
#if defined(__unix__) || defined(__APPLE__)
  const int dir_descriptor = ::open(directory_path.c_str(), O_RDONLY | O_DIRECTORY);
  if (dir_descriptor >= 0) {
    (void)::fsync(dir_descriptor);
    (void)::close(dir_descriptor);
  }
#else
  (void)directory_path;
#endif
}

void atomic_durable_rename(const std::filesystem::path& temp_path,
                           const std::filesystem::path& destination) {
  // Step 1: Force data & inode/metadata flush of temporary file to disk
  sync_file(temp_path);

  // Step 2: Atomic rename
  std::error_code error;
  std::filesystem::rename(temp_path, destination, error);
  if (error) {
    std::filesystem::remove(temp_path);
    throw std::runtime_error("cannot atomically replace file: " + error.message());
  }

  // Step 3: Force directory entry flush on parent directory
  sync_directory(destination.parent_path());
}

}  // namespace streamforge::storage
