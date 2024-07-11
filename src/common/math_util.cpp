#include "common/math_util.h"

#include <cmath>

namespace noisepage::common {

auto MathUtil::ApproxEqual(float left, float right) -> bool {
    const double epsilon = std::fabs(right) * 0.01;
    return std::fabs(left - right) <= epsilon;
}

auto MathUtil::ApproxEqual(double left, double right) -> bool {
    const double epsilon = std::fabs(right) * 0.01;
    return std::fabs(left - right) <= epsilon;
}

} // namespace noisepage::common
