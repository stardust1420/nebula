package nebula

import (
	"fmt"
	"hash/fnv"
	"sort"
)

// defaultVirtualNodes is the number of points each worker occupies on the ring.
// More virtual nodes give a smoother key distribution at the cost of a slightly
// larger ring.
const defaultVirtualNodes = 100

// hashRing is a classic consistent-hashing ring. Each worker is placed on the
// ring at several virtual-node positions; a key is routed to the first worker
// found clockwise from the key's hash. This keeps a key mapped to the same
// worker for the lifetime of the ring and spreads keys evenly across workers.
type hashRing struct {
	points   []uint32          // sorted virtual-node hash positions
	nodeByPt map[uint32]uint64 // hash position -> worker index
}

// newHashRing builds a ring for the given number of workers. If numWorkers is
// zero the ring is empty and get always returns 0 (callers must guard against
// having no workers).
func newHashRing(numWorkers uint64, virtualNodes int) *hashRing {
	r := &hashRing{nodeByPt: make(map[uint32]uint64)}
	for w := uint64(0); w < numWorkers; w++ {
		for v := 0; v < virtualNodes; v++ {
			pt := hashKey(fmt.Sprintf("worker-%d-vnode-%d", w, v))
			if _, exists := r.nodeByPt[pt]; exists {
				// Extremely unlikely collision; skip to keep the mapping stable.
				continue
			}
			r.nodeByPt[pt] = w
			r.points = append(r.points, pt)
		}
	}
	sort.Slice(r.points, func(i, j int) bool { return r.points[i] < r.points[j] })
	return r
}

// get returns the worker index responsible for key.
func (r *hashRing) get(key string) uint64 {
	if len(r.points) == 0 {
		return 0
	}
	h := hashKey(key)
	// First point clockwise (>= h); wrap around to the start of the ring.
	idx := sort.Search(len(r.points), func(i int) bool { return r.points[i] >= h })
	if idx == len(r.points) {
		idx = 0
	}
	return r.nodeByPt[r.points[idx]]
}

// hashKey hashes a string to a 32-bit ring position using FNV-1a.
func hashKey(s string) uint32 {
	h := fnv.New32a()
	// Hash.Write never returns an error.
	_, _ = h.Write([]byte(s))
	return h.Sum32()
}
