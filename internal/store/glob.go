package store

// Match reports whether s matches the Redis glob-style pattern:
//
//   - any sequence of bytes (including empty)
//     ?       exactly one byte
//     [abc]   one of the listed bytes
//     [a-z]   a byte in the range (reversed ranges such as [z-a] are accepted)
//     [^a]    any byte not listed
//     \x      the literal byte x
//
// An unterminated '[' class extends to the end of the pattern, as in Redis.
// Matching is byte-wise and runs in O(len(pattern)*len(s)) worst case thanks
// to single-star backtracking (no exponential blowup on "*a*a*a*b").
func Match(pattern, s string) bool {
	p, i := 0, 0
	starP, starI := -1, 0
	for i < len(s) {
		if p < len(pattern) {
			switch pattern[p] {
			case '*':
				for p < len(pattern) && pattern[p] == '*' {
					p++
				}
				if p == len(pattern) {
					return true
				}
				starP, starI = p, i
				continue
			case '?':
				p++
				i++
				continue
			case '[':
				if ok, next := matchClass(pattern, p, s[i]); ok {
					p = next
					i++
					continue
				}
			case '\\':
				lit, step := byte('\\'), 1
				if p+1 < len(pattern) {
					lit, step = pattern[p+1], 2
				}
				if lit == s[i] {
					p += step
					i++
					continue
				}
			default:
				if pattern[p] == s[i] {
					p++
					i++
					continue
				}
			}
		}
		// Mismatch: let the last '*' swallow one more byte and retry.
		if starP >= 0 {
			starI++
			i = starI
			p = starP
			continue
		}
		return false
	}
	for p < len(pattern) && pattern[p] == '*' {
		p++
	}
	return p == len(pattern)
}

// matchClass evaluates the class starting at pat[p] == '[' against c and
// returns whether it matched plus the pattern index just after the class.
func matchClass(pat string, p int, c byte) (bool, int) {
	p++ // skip '['
	negate := false
	if p < len(pat) && pat[p] == '^' {
		negate = true
		p++
	}
	matched := false
	for p < len(pat) {
		switch {
		case pat[p] == '\\' && p+1 < len(pat):
			p++
			if pat[p] == c {
				matched = true
			}
			p++
		case pat[p] == ']':
			if negate {
				matched = !matched
			}
			return matched, p + 1
		case p+2 < len(pat) && pat[p+1] == '-':
			lo, hi := pat[p], pat[p+2]
			if lo > hi {
				lo, hi = hi, lo
			}
			if c >= lo && c <= hi {
				matched = true
			}
			p += 3
		default:
			if pat[p] == c {
				matched = true
			}
			p++
		}
	}
	if negate {
		matched = !matched
	}
	return matched, len(pat)
}
