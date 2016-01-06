package stats

import "fmt"
import "math"

type Stats struct {
	Avg float64
	Min float64
	Max float64
	P50 float64
	P75 float64
	P90 float64
	P95 float64
	P99 float64
}

const msFactor = 1000000

// Accepts a sorted slice of durations in nanoseconds
// Returns a Stats struct of millisecond statistics
func Get(data []int) Stats {
	min, max := minmax(data)

	return Stats{
		Avg: avg(data) / msFactor,
		Min: min / msFactor,
		Max: max / msFactor,
		P50: p(data, 0.5) / msFactor,
		P75: p(data, 0.75) / msFactor,
		P90: p(data, 0.9) / msFactor,
		P95: p(data, 0.95) / msFactor,
		P99: p(data, 0.99) / msFactor,
	}
}

func minmax(data []int) (float64, float64) {
	min := int(math.MaxInt64)
	max := int(math.MinInt64)
	for _, d := range data {
		if d < min {
			min = d
		}
		if d > max {
			max = d
		}
	}
	return float64(min), float64(max)
}

func avg(data []int) float64 {
	sum := float64(0)
	for _, d := range data {
		sum += float64(d)
	}
	return sum / float64(len(data))
}

func p(data []int, p float64) float64 {
	idx := pIdx(data, p)
	return float64(data[idx])
}

func pIdx(data []int, p float64) int {
	return int(math.Ceil(float64(len(data)) * p))
}

const numBuckets = 100
const maxHeight = 50

// Accepts a sorted slice of durations in nanoseconds
// Prints a histogram to stdout
func PrintHist(data []int) {
	// Cut the data at the 99th percentile
	p99Idx := pIdx(data, 0.99)
	data = data[:p99Idx]

	buckets := make([]int, numBuckets)
	min := data[0]
	max := data[len(data)-1]
	step := float64(max-min) / numBuckets
	prevCutIdx := 0
	maxBucket := 0

	for i := 0; i < numBuckets; i++ {
		cut := float64(min) + step*float64(i+1)
		count := 0
		j := prevCutIdx

		for ; j < len(data) && float64(data[j]) < cut; j++ {
			count++
		}

		prevCutIdx = j
		buckets[i] = count

		if count > maxBucket {
			maxBucket = count
		}
	}

	// Scale the graph to a reasonable maximum height
	heightRatio := float64(maxHeight) / float64(maxBucket)
	for i := 0; i < len(buckets); i++ {
		buckets[i] = int(math.Ceil(float64(buckets[i]) * heightRatio))
	}

	// Scan downward over the histogram printing line by line
	hist := make([]rune, 0)
	for i := maxHeight; i >= 0; i-- {
		for j := 0; j < numBuckets; j++ {
			if i == 0 {
				hist = append(hist, '=')
			} else if buckets[j] == i {
				hist = append(hist, '|')
				buckets[j]--
			} else {
				hist = append(hist, ' ')
			}
		}

		hist = append(hist, '\n')
	}

	gmin := float64(min) / msFactor
	gmax := float64(max) / msFactor
	gmid := gmin + (gmax-gmin)/2

	pointerRow := make([]rune, numBuckets)

	for i := 1; i < numBuckets-1; i++ {
		pointerRow[i] = ' '
	}

	pointerRow[0] = '^'
	pointerRow[len(pointerRow)/2] = '^'
	pointerRow[len(pointerRow)-1] = '^'

	fmt.Print(string(hist))
	fmt.Println(string(pointerRow))
	fmt.Printf("%.4fms                                       %.4fms                                     %.4fms\n", gmin, gmid, gmax)
}
