// Expected output: 6

fun main() -> int {
  var ret = 0

  // ----------------------
  // TRUE <=> TRUE
  // ----------------------
  if (true == true) { // TRUE
      ret = ret + 1
  }
  if (true != true) { // FALSE
      return -1
  }
  if (true > true) { // FALSE
      return -1
  }
  if (true >= true) { // TRUE
      ret = ret + 1
  }
  if (true < true) { // FALSE
      return -1
  }
  if (true <= true) { // TRUE
      ret = ret + 1
  }

  // ----------------------
  // TRUE <=> FALSE
  // ----------------------
  if (true == false) { // FALSE
      return -1
  }
  if (true != false) { // TRUE
      ret = ret + 1
  }
  if (true > false) { // TRUE
      ret = ret + 1
  }
  if (true >= false) { // TRUE
      ret = ret + 1
  }
  if (true < false) { // FALSE
      return -1
  }
  if (true <= false) { // FALSE
      return -1
  }

  return ret
}
