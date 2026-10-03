package ring

import (
	"fmt"
	"sort"

	"github.com/cespare/xxhash/v2"
)

// Ring is an immutable consistent hash ring that maps string keys to partition IDs.
// It uses xxhash and virtual nodes for even distribution. Safe for concurrent reads
// after construction — no locks needed.
type Ring struct {
	partitions int
	tokens     []uint32
	owners     []int32
}

// NewRing creates a consistent hash ring with the given number of partitions
// and virtual nodes per partition. Tokens are generated using xxhash.
// If partitions or virtualNodesPerPartition are <= 0, returns a ring that
// maps everything to partition 0.
func NewRing(partitions int, virtualNodesPerPartition int) *Ring {
	r := &Ring{partitions: partitions}

	if partitions <= 0 || virtualNodesPerPartition <= 0 {
		return r
	}

	total := partitions * virtualNodesPerPartition
	r.tokens = make([]uint32, 0, total)
	r.owners = make([]int32, 0, total)

	for p := 0; p < partitions; p++ {
		for v := 0; v < virtualNodesPerPartition; v++ {
			token := uint32(xxhash.Sum64String(fmt.Sprintf("partition-%d-vnode-%d", p, v)))
			r.tokens = append(r.tokens, token)
			r.owners = append(r.owners, int32(p))
		}
	}

	// Sort tokens and reorder owners in parallel.
	sortRing(r.tokens, r.owners)

	return r
}

// GetPartition returns the partition ID for the given key.
// Uses binary search over sorted tokens (O(log N) lookup).
// If the ring is empty (0 tokens), returns 0.
func (r *Ring) GetPartition(key string) int32 {
	if len(r.tokens) == 0 {
		return 0
	}

	hash := uint32(xxhash.Sum64String(key))

	// Find the first token >= hash.
	idx := sort.Search(len(r.tokens), func(i int) bool {
		return r.tokens[i] >= hash
	})

	// Wrap around: if hash is above all tokens, use the first token.
	if idx == len(r.tokens) {
		idx = 0
	}

	return r.owners[idx]
}

// GetPartitionForTenant returns the owning partition for a tenant.
// Routing is per-tenant only — labels are not part of the routing key.
func (r *Ring) GetPartitionForTenant(tenantID string) int32 {
	return r.GetPartition(tenantID)
}

// sortRing sorts tokens ascending and reorders owners in parallel.
func sortRing(tokens []uint32, owners []int32) {
	// Build index pairs, sort by token, then unpack.
	type pair struct {
		token uint32
		owner int32
	}
	pairs := make([]pair, len(tokens))
	for i := range tokens {
		pairs[i] = pair{tokens[i], owners[i]}
	}
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].token < pairs[j].token
	})
	for i, p := range pairs {
		tokens[i] = p.token
		owners[i] = p.owner
	}
}
