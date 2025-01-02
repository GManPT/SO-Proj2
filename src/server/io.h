#ifndef KVS_IO_H
#define KVS_IO_H

#include <unistd.h>

/// Writes an unsigned integer to the given file descriptor.
/// @param fd The file descriptor to write to.
/// @param value The value to write.
void write_uint(int fd, int value);

/// @brief Copies bytes from src to dest, not including the '\0'
/// @param dest 
/// @param src 
/// @param n Maximum number of bytes to copy from src to dest
/// @return Number of bytes copied
size_t strn_memcpy(char* dest, const char* src, size_t n);

#endif  // KVS_IO_H
