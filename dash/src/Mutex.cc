#include <dash/Mutex.h>
#include <dash/Exception.h>

namespace dash {

Mutex::Mutex(Team & team){
  if (team != dash::Team::Null()) {
    dart_ret_t ret = dart_team_lock_init(team.dart_id(), &_mutex);
    DASH_ASSERT_EQ(DART_OK, ret, "dart_team_lock_init failed");
  }
}

Mutex::~Mutex(){
  dart_ret_t ret = dart_team_lock_destroy(&_mutex);
  if (ret != DART_OK) {
    DASH_LOG_ERROR("Failed to destroy DART lock! "
                   "(dart_team_lock_free failed)");
  }
}

bool Mutex::init(Team const& team)
{
  bool flag;
  DASH_ASSERT_RETURNS(dart_lock_initialized(_mutex, &flag), DART_OK);

  if (flag) {
    DASH_LOG_ERROR("DART lock already initialized!");
  } else {
    int ret = dart_team_lock_init(team.dart_id(), &_mutex);
    flag = ret == DART_OK;

    if (ret != DART_OK) {
      DASH_LOG_ERROR("dart_team_lock_init failed", ret);
    }
  }

  return flag;
}

void Mutex::lock(){
  dart_ret_t ret = dart_lock_acquire(_mutex);
  DASH_ASSERT_EQ(DART_OK, ret, "dart_lock_acquire failed");
}

bool Mutex::try_lock(){
  int32_t result;
  dart_ret_t ret = dart_lock_try_acquire(_mutex, &result);
  DASH_ASSERT_EQ(DART_OK, ret, "dart_lock_try_acquire failed");
  return static_cast<bool>(result);
}

void Mutex::unlock(){
  dart_ret_t ret = dart_lock_release(_mutex);
  DASH_ASSERT_EQ(DART_OK, ret, "dart_lock_acquire failed");
}

} // namespace dash
