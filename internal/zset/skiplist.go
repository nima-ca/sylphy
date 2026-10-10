package zset

// maxLevel caps node height. With branching 4, 2^32 elements still fit
// comfortably under it, which is Redis's choice too.
const maxLevel = 32

// branching is the inverse of the promotion probability: a node reaches the
// next level with probability 1/branching = 0.25.
const branching = 4

// level is one forward pointer of a node. span is the number of level-0 steps
// the pointer jumps, which is what makes rank queries O(log n): summing spans
// along a search path gives the rank of the node reached.
//
// For a nil forward the span is not meaningful, but insert and deleteNode keep
// adjusting it symmetrically so the arithmetic stays uniform.
type level struct {
	forward *node
	span    int
}

type node struct {
	member   string
	score    float64
	backward *node // previous node on level 0; nil for the first node
	levels   []level
}

// skipList is an ordered sequence of (score, member) pairs with spans.
// Ranks are 1-based here (the header is rank 0); ZSet converts to 0-based.
//
// Invariants: level is in [1, maxLevel]; header has maxLevel levels; every
// level above skipList.level is empty; when level > 1 the top level is
// non-empty; tail is the last node (nil when empty); length counts nodes.
type skipList struct {
	header *node
	tail   *node
	length int
	level  int
	intN   func(n int) int
}

func newSkipList(intN func(n int) int) *skipList {
	return &skipList{
		header: &node{levels: make([]level, maxLevel)},
		level:  1,
		intN:   intN,
	}
}

// before reports whether (s1, m1) sorts strictly before (s2, m2): by score,
// then by member bytes (Go string comparison is bytewise).
func before(s1 float64, m1 string, s2 float64, m2 string) bool {
	return s1 < s2 || (s1 == s2 && m1 < m2)
}

func (sl *skipList) randomLevel() int {
	lvl := 1
	for lvl < maxLevel && sl.intN(branching) == 0 {
		lvl++
	}
	return lvl
}

// predecessors returns, for every level below sl.level, the last node that
// sorts strictly before (score, member).
func (sl *skipList) predecessors(score float64, member string) (update [maxLevel]*node) {
	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		for f := x.levels[i].forward; f != nil && before(f.score, f.member, score, member); f = x.levels[i].forward {
			x = f
		}
		update[i] = x
	}
	return update
}

// insert adds (score, member), which must not be present, in O(log n).
func (sl *skipList) insert(score float64, member string) {
	var update [maxLevel]*node
	var rank [maxLevel]int // rank[i]: rank of update[i]
	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		if i != sl.level-1 {
			rank[i] = rank[i+1]
		}
		for f := x.levels[i].forward; f != nil && before(f.score, f.member, score, member); f = x.levels[i].forward {
			rank[i] += x.levels[i].span
			x = f
		}
		update[i] = x
	}

	lvl := sl.randomLevel()
	if lvl > sl.level {
		for i := sl.level; i < lvl; i++ {
			rank[i] = 0
			update[i] = sl.header
			update[i].levels[i].span = sl.length // header jumps over everything
		}
		sl.level = lvl
	}

	n := &node{member: member, score: score, levels: make([]level, lvl)}
	for i := 0; i < lvl; i++ {
		n.levels[i].forward = update[i].levels[i].forward
		update[i].levels[i].forward = n
		// Split the predecessor's span around the new node.
		n.levels[i].span = update[i].levels[i].span - (rank[0] - rank[i])
		update[i].levels[i].span = (rank[0] - rank[i]) + 1
	}
	// Levels the new node does not reach now jump over one more node.
	for i := lvl; i < sl.level; i++ {
		update[i].levels[i].span++
	}

	if update[0] != sl.header {
		n.backward = update[0]
	}
	if f := n.levels[0].forward; f != nil {
		f.backward = n
	} else {
		sl.tail = n
	}
	sl.length++
}

// deleteNode unlinks x. update must hold x's predecessors on every level.
func (sl *skipList) deleteNode(x *node, update *[maxLevel]*node) {
	for i := 0; i < sl.level; i++ {
		if update[i].levels[i].forward == x {
			update[i].levels[i].span += x.levels[i].span - 1
			update[i].levels[i].forward = x.levels[i].forward
		} else {
			update[i].levels[i].span--
		}
	}
	if f := x.levels[0].forward; f != nil {
		f.backward = x.backward
	} else {
		sl.tail = x.backward
	}
	for sl.level > 1 && sl.header.levels[sl.level-1].forward == nil {
		sl.level--
	}
	sl.length--
	// Drop the node's links so a stale pointer cannot keep neighbors alive.
	x.backward = nil
	for i := range x.levels {
		x.levels[i].forward = nil
	}
}

// remove deletes (score, member) and reports whether it was present.
func (sl *skipList) remove(score float64, member string) bool {
	update := sl.predecessors(score, member)
	x := update[0].levels[0].forward
	if x == nil || x.score != score || x.member != member {
		return false
	}
	sl.deleteNode(x, &update)
	return true
}

// removeRun deletes first and the nodes after it for as long as ok(node, i)
// holds (i counts from 0 within the run), in O(log n + k). The predecessor
// array computed once for first stays valid while the run is cut out, because
// each deleted node is the successor of the same predecessors. It returns the
// removed pairs in ascending order.
func (sl *skipList) removeRun(first *node, ok func(n *node, i int) bool) []Entry {
	if first == nil {
		return nil
	}
	update := sl.predecessors(first.score, first.member)
	var out []Entry
	cur := first
	for i := 0; cur != nil && ok(cur, i); i++ {
		next := cur.levels[0].forward
		sl.deleteNode(cur, &update)
		out = append(out, Entry{Member: cur.member, Score: cur.score})
		cur = next
	}
	return out
}

// rank returns the 1-based rank of (score, member), or 0 if absent.
func (sl *skipList) rank(score float64, member string) int {
	x := sl.header
	rank := 0
	for i := sl.level - 1; i >= 0; i-- {
		for f := x.levels[i].forward; f != nil && !before(score, member, f.score, f.member); f = x.levels[i].forward {
			rank += x.levels[i].span
			x = f
		}
		if x != sl.header && x.score == score && x.member == member {
			return rank
		}
	}
	return 0
}

// nodeByRank returns the node at 1-based rank, or nil if out of range.
func (sl *skipList) nodeByRank(rank int) *node {
	if rank < 1 || rank > sl.length {
		return nil
	}
	x := sl.header
	traversed := 0
	for i := sl.level - 1; i >= 0; i-- {
		for x.levels[i].forward != nil && traversed+x.levels[i].span <= rank {
			traversed += x.levels[i].span
			x = x.levels[i].forward
		}
		if traversed == rank {
			return x
		}
	}
	return nil
}

// firstInRange returns the first node whose score lies in r and its 1-based
// rank; nil if none. Nodes below the lower bound form a prefix of the list, so
// the search descends like any skip-list lookup.
func (sl *skipList) firstInRange(r ScoreRange) (*node, int) {
	if r.empty() || sl.length == 0 {
		return nil, 0
	}
	x := sl.header
	rank := 0
	for i := sl.level - 1; i >= 0; i-- {
		for f := x.levels[i].forward; f != nil && !r.aboveMin(f.score); f = x.levels[i].forward {
			rank += x.levels[i].span
			x = f
		}
	}
	n := x.levels[0].forward
	if n == nil || !r.belowMax(n.score) {
		return nil, 0
	}
	return n, rank + 1
}

// lastInRange is the mirror of firstInRange.
func (sl *skipList) lastInRange(r ScoreRange) (*node, int) {
	if r.empty() || sl.length == 0 {
		return nil, 0
	}
	x := sl.header
	rank := 0
	for i := sl.level - 1; i >= 0; i-- {
		for f := x.levels[i].forward; f != nil && r.belowMax(f.score); f = x.levels[i].forward {
			rank += x.levels[i].span
			x = f
		}
	}
	if x == sl.header || !r.aboveMin(x.score) {
		return nil, 0
	}
	return x, rank
}
