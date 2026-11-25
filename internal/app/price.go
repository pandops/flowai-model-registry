package app

import (
	"encoding/json"
	"fmt"
	"math/big"
	"regexp"
	"strings"
)

var usdPattern = regexp.MustCompile(`^(0|[1-9][0-9]*)(\.[0-9]{1,6})?$`)

var (
	million  = big.NewRat(1_000_000, 1)
	priceMax = big.NewRat(1_000_000, 1)
)

// parsePrice parses a USD-per-million-tokens decimal string.
func parsePrice(s string) (*big.Rat, bool) {
	if !usdPattern.MatchString(s) {
		return nil, false
	}
	r, ok := new(big.Rat).SetString(s)
	if !ok || r.Cmp(priceMax) > 0 {
		return nil, false
	}
	return r, true
}

// trimDecimal removes trailing fractional zeros.
func trimDecimal(s string) string {
	if strings.Contains(s, ".") {
		s = strings.TrimRight(s, "0")
		s = strings.TrimSuffix(s, ".")
	}
	return s
}

// perToken converts USD per million tokens to an exact per-token number.
func perToken(perMillion *big.Rat) json.Number {
	return json.Number(trimDecimal(new(big.Rat).Quo(perMillion, million).FloatString(12)))
}

// upstreamRat reads a numeric LiteLLM value without rounding.
func upstreamRat(v any) (*big.Rat, bool) {
	var s string
	switch n := v.(type) {
	case json.Number:
		s = n.String()
	case float64:
		s = fmt.Sprint(n)
	case string:
		s = n
	default:
		return nil, false
	}
	r, ok := new(big.Rat).SetString(s)
	return r, ok
}

// perMillionString projects a per-token LiteLLM rate; ok is false when the
// value is malformed, negative, above the range or has more than six
// fractional digits per million tokens.
func perMillionString(v any) (string, bool) {
	r, ok := upstreamRat(v)
	if !ok || r.Sign() < 0 {
		return "", false
	}
	pm := new(big.Rat).Mul(r, million)
	if pm.Cmp(priceMax) > 0 || !new(big.Rat).Mul(pm, million).IsInt() {
		return "", false
	}
	return trimDecimal(pm.FloatString(6)), true
}

// pricingOf projects litellm_params into the public pricing pair.
func pricingOf(params map[string]any) (any, string) {
	in, hasIn := params["input_cost_per_token"]
	out, hasOut := params["output_cost_per_token"]
	var inS, outS string
	okIn, okOut := true, true
	if hasIn {
		inS, okIn = perMillionString(in)
	}
	if hasOut {
		outS, okOut = perMillionString(out)
	}
	switch {
	case !okIn || !okOut:
		return nil, "unsupported"
	case hasIn && hasOut:
		return map[string]any{"input_usd_per_million_tokens": inS, "output_usd_per_million_tokens": outS}, "configured"
	default:
		return nil, "not_configured"
	}
}

// sameNumber compares two numeric values exactly.
func sameNumber(a, b any) bool {
	x, ok1 := upstreamRat(a)
	y, ok2 := upstreamRat(b)
	return ok1 && ok2 && x.Cmp(y) == 0
}

// usdOf normalizes a LiteLLM USD amount to at most six fractional digits.
func usdOf(v json.Number) (string, bool) {
	r, ok := new(big.Rat).SetString(v.String())
	if !ok {
		return "", false
	}
	if r.Sign() < 0 {
		r.SetInt64(0)
	}
	return trimDecimal(r.FloatString(6)), true
}

// decimalNumber turns a decimal string into a JSON number.
func decimalNumber(s string) (json.Number, bool) {
	r, ok := new(big.Rat).SetString(s)
	if !ok {
		return "", false
	}
	return json.Number(trimDecimal(r.FloatString(6))), true
}

// atLeast reports whether spend >= budget.
func atLeast(spend json.Number, budget string) bool {
	s, ok1 := new(big.Rat).SetString(spend.String())
	b, ok2 := new(big.Rat).SetString(budget)
	return ok1 && ok2 && s.Cmp(b) >= 0
}
