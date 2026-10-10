package zset

import (
	"fmt"
	"math/rand/v2"
	"strconv"
	"testing"
)

var benchSizes = []int{1_000, 100_000}

// benchSet builds a set of n members with random scores in [0, 1e6).
func benchSet(n int) (*ZSet, []string) {
	r := rand.New(rand.NewPCG(1, 2))
	z := NewWithRand(r.IntN)
	members := make([]string, n)
	for i := range members {
		members[i] = "member:" + strconv.Itoa(i)
		z.Set(members[i], float64(r.IntN(1_000_000)))
	}
	return z, members
}

func BenchmarkInsert(b *testing.B) {
	r := rand.New(rand.NewPCG(3, 4))
	members := make([]string, b.N)
	for i := range members {
		members[i] = "member:" + strconv.Itoa(i)
	}
	z := NewWithRand(r.IntN)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		z.Set(members[i], float64(r.IntN(1_000_000)))
	}
}

func BenchmarkUpdateScore(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			z, members := benchSet(n)
			r := rand.New(rand.NewPCG(5, 6))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				z.Set(members[i%n], float64(r.IntN(1_000_000)))
			}
		})
	}
}

func BenchmarkRank(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			z, members := benchSet(n)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				z.Rank(members[(i*7919)%n])
			}
		})
	}
}

func BenchmarkRangeByRank100(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			z, _ := benchSet(n)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				lo := (i * 7919) % (n - 100)
				z.RangeByRank(lo, lo+100, false)
			}
		})
	}
}

func BenchmarkRangeByScore(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			z, _ := benchSet(n)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				lo := float64((i * 7919) % 990_000)
				z.RangeByScore(ScoreRange{Min: lo, Max: lo + 10_000}, false, 0, 100)
			}
		})
	}
}

func BenchmarkCount(b *testing.B) {
	z, _ := benchSet(100_000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lo := float64((i * 7919) % 900_000)
		z.Count(ScoreRange{Min: lo, Max: lo + 50_000})
	}
}

// BenchmarkPopMinAndReinsert is the priority-queue pattern: pop the lowest
// entry and put it back with a new score.
func BenchmarkPopMinAndReinsert(b *testing.B) {
	z, _ := benchSet(100_000)
	r := rand.New(rand.NewPCG(7, 8))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e := z.PopMin(1)[0]
		z.Set(e.Member, float64(r.IntN(1_000_000)))
	}
}
