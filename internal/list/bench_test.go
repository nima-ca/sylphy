package list

import (
	"fmt"
	"testing"
)

var benchVal = []byte("0123456789abcdef")

func benchList(n int) *List {
	l := New()
	for i := 0; i < n; i++ {
		l.PushBack(benchVal)
	}
	return l
}

// BenchmarkQueue is the work-queue pattern at a steady depth: push at the
// tail, pop at the head.
func BenchmarkQueue(b *testing.B) {
	l := benchList(1000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.PushBack(benchVal)
		l.PopFront()
	}
}

// BenchmarkStack pushes and pops at the same end.
func BenchmarkStack(b *testing.B) {
	l := benchList(1000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.PushFront(benchVal)
		l.PopFront()
	}
}

func BenchmarkAt(b *testing.B) {
	for _, n := range []int{1_000, 100_000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			l := benchList(n)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				l.At((i * 7919) % n)
			}
		})
	}
}

func BenchmarkRange100(b *testing.B) {
	l := benchList(100_000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lo := (i * 7919) % 99_000
		l.Range(lo, lo+99)
	}
}

// BenchmarkGrowAndDrain measures the ring buffer's resize and shrink path.
func BenchmarkGrowAndDrain(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		l := New()
		for j := 0; j < 10_000; j++ {
			l.PushBack(benchVal)
		}
		for j := 0; j < 10_000; j++ {
			l.PopFront()
		}
	}
}
