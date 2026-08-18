package stress

// ParseFactor exposes the environment-value parser to the (external) test so
// the multiplier rules can be asserted without a subprocess.
var ParseFactor = parseFactor
