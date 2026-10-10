package zset

import "fmt"

// check verifies every skip-list invariant and returns the first violation.
func (sl *skipList) check() error {
	ranks := map[*node]int{sl.header: 0}
	var prev *node
	n := 0
	for x := sl.header.levels[0].forward; x != nil; x = x.levels[0].forward {
		n++
		ranks[x] = n
		if len(x.levels) < 1 || len(x.levels) > maxLevel {
			return fmt.Errorf("node %q has %d levels", x.member, len(x.levels))
		}
		if x.backward != prev {
			return fmt.Errorf("node %q: wrong backward pointer", x.member)
		}
		if prev != nil && !before(prev.score, prev.member, x.score, x.member) {
			return fmt.Errorf("order violated between %q and %q", prev.member, x.member)
		}
		prev = x
	}
	if n != sl.length {
		return fmt.Errorf("level 0 holds %d nodes, length says %d", n, sl.length)
	}
	if sl.tail != prev {
		return fmt.Errorf("tail does not point at the last node")
	}
	if sl.level < 1 || sl.level > maxLevel {
		return fmt.Errorf("level %d out of range", sl.level)
	}
	if sl.level > 1 && sl.header.levels[sl.level-1].forward == nil {
		return fmt.Errorf("top level %d is empty", sl.level)
	}
	for i := 0; i < maxLevel; i++ {
		if i >= sl.level {
			if sl.header.levels[i].forward != nil {
				return fmt.Errorf("level %d is above skipList.level but not empty", i)
			}
			continue
		}
		cur := sl.header
		for f := cur.levels[i].forward; f != nil; f = cur.levels[i].forward {
			if len(f.levels) <= i {
				return fmt.Errorf("node %q linked on level %d but has %d levels", f.member, i, len(f.levels))
			}
			if got, want := cur.levels[i].span, ranks[f]-ranks[cur]; got != want {
				return fmt.Errorf("level %d: span before %q is %d, want %d", i, f.member, got, want)
			}
			cur = f
		}
	}
	return nil
}

// check verifies the skip list and that the member map mirrors it.
func (z *ZSet) check() error {
	if err := z.sl.check(); err != nil {
		return err
	}
	if len(z.dict) != z.sl.length {
		return fmt.Errorf("map has %d members, skip list %d", len(z.dict), z.sl.length)
	}
	for x := z.sl.header.levels[0].forward; x != nil; x = x.levels[0].forward {
		if s, ok := z.dict[x.member]; !ok || s != x.score {
			return fmt.Errorf("member %q: map says (%v, %v), list says %v", x.member, s, ok, x.score)
		}
	}
	return nil
}
