#pragma once

#include <filesystem>

namespace streamforge::storage {

// Flushes and synchronizes file data and metadata to persistent storage.
// Uses fsync on POSIX and FlushFileBuffers on Windows.
void sync_file(const std::filesystem::path& path);

// Synchronizes directory metadata to persistent storage.
// On POSIX, opens the directory descriptor and calls fsync.
// On Windows, directory metadata commits are handled by NTFS transaction semantics.
void sync_directory(const std::filesystem::path& directory_path);

// Durably and atomically replaces destination with temp_path:
// 1. fsync(temp_path) / FlushFileBuffers
// 2. rename(temp_path, destination)
// 3. fsync(parent_directory)
void atomic_durable_rename(const std::filesystem::path& temp_path,
                           const std::filesystem::path& destination);

}  // namespace streamforge::storage
