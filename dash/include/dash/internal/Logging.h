#ifndef DASH__INTERNAL__LOGGING_H_
#define DASH__INTERNAL__LOGGING_H_

#include <dash/internal/Macro.h>

#if defined(DASH_ENABLE_LOGGING) && \
    defined(DASH_ENABLE_TRACE_LOGGING)

#include <sstream>
#include <iostream>
#include <iterator>

#define DASH_LOG_TRACE(...) \
  dash::internal::logging::LogWrapper(\
    __FILE__, __LINE__, __VA_ARGS__)

namespace dash {
namespace internal {
namespace logging {

// To print std::array to ostream
template <class T, std::size_t N>
std::ostream & operator<<(
  std::ostream & o,
  const std::array<T, N> & arr) {
  std::copy(arr.cbegin(), arr.cend(),
            std::ostream_iterator<T>(o, ","));
  return o;
}

// Terminator
static void Log_Recursive(
  const char* file,
  int line,
  const char* context_tag,
  std::ostringstream & msg) {
  std::cout << "[  TRACE   ][" << context_tag << "] "
            << file << ":" << line << " "
            << msg.str() << std::endl;
}

// "Recursive" variadic function
template<typename T, typename... Args>
static void Log_Recursive(
  const char* file,
  int line,
  const char* context_tag,
  std::ostringstream & msg,
  T value,
  const Args & ... args) {
  msg << value << " ";
  Log_Recursive(file, line, context_tag, msg, args...);
}

// Log_Recursive wrapper that creates the ostringstream
template<typename... Args>
static void LogWrapper(
  const char* filepath,
  int line,
  const char* context_tag,
  const Args & ... args) {
  std::ostringstream msg;
  // Extract file name from path
  std::string filename(filepath);
  std::size_t offset = filename.find_last_of("/\\");
  Log_Recursive(
    filename.substr(offset+1).c_str(),
    line,
    context_tag,
    msg, args...);
}

} // namespace logging
} // namespace internal
} // namespace dash

#else 

#define DASH_LOG_TRACE(...) do {  } while(0)

#endif // DASH_ENABLE_LOGGING

#endif // DASH__INTERNAL__LOGGING_H_