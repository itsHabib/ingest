package index

import (
	"time"
)

// BlockMeta captures metadata about a single flushed batch file.
type BlockMeta struct {
	FilePath   string            `json:"file_path"`
	TenantID   string            `json:"tenant_id"`
	Partition  int32             `json:"partition"`
	MinTime    time.Time         `json:"min_time"`
	MaxTime    time.Time         `json:"max_time"`
	EventCount int               `json:"event_count"`
	Labels     map[string]string `json:"labels"`
	SizeBytes  int               `json:"size_bytes"`
}

// Index aggregates BlockMeta entries for a single tenant on a single day.
type Index struct {
	TenantID string      `json:"tenant_id"`
	Date     string      `json:"date"` // YYYY-MM-DD
	Blocks   []BlockMeta `json:"blocks"`
}

// NewIndex creates an empty index for the given tenant and date.
func NewIndex(tenantID, date string) *Index {
	return &Index{
		TenantID: tenantID,
		Date:     date,
	}
}

// AddBlock appends a block to the index.
func (idx *Index) AddBlock(meta BlockMeta) {
	idx.Blocks = append(idx.Blocks, meta)
}

// Lookup returns blocks whose time range overlaps [start, end] and whose
// labels could match the given matchers. A block is included if:
//  1. Its [MinTime, MaxTime] overlaps with [start, end], AND
//  2. For every matcher, the block's label set contains the matcher's label name
//     and the value satisfies the matcher (or the matcher is a not-equal/not-regex
//     type, in which case absence of the label is considered a match).
//
// This may return false positives (blocks that don't actually contain matching
// events) but never false negatives.
func (idx *Index) Lookup(start, end time.Time, matchers []LabelMatcher) []BlockMeta {
	var result []BlockMeta
	for _, b := range idx.Blocks {
		if !timeOverlaps(b.MinTime, b.MaxTime, start, end) {
			continue
		}
		if !blockMatchesLabels(b.Labels, matchers) {
			continue
		}
		result = append(result, b)
	}
	return result
}

// timeOverlaps returns true if [bMin, bMax] overlaps with [start, end].
func timeOverlaps(bMin, bMax, start, end time.Time) bool {
	return !bMax.Before(start) && !bMin.After(end)
}

// blockMatchesLabels checks whether a block's label set could contain events
// matching all the given matchers. For negative matchers (NotEqual, NotRegexp),
// an absent label is treated as a potential match (conservative / no false negatives).
func blockMatchesLabels(labels map[string]string, matchers []LabelMatcher) bool {
	for _, m := range matchers {
		val, exists := labels[m.Name]
		if !exists {
			// Label not in block's label set.
			// For negative matchers, absence means possible match.
			// For positive matchers (Equal, Regexp), absence means no event
			// in this block has this label, so it could still match if the
			// matcher matches empty string.
			switch m.Type {
			case MatchNotEqual, MatchNotRegexp:
				continue // absence is a potential match
			default:
				// For Equal/Regexp, check if the matcher matches empty string.
				// If it does, the block could contain events without this label.
				if !m.Matches("") {
					return false
				}
			}
			continue
		}
		// Label exists in block. For block-level filtering, the block's label
		// value is the union of all event label values. Since we store only
		// one value per key (not all unique values), we can only do an exact
		// check. If the block has the key, we conservatively include it to
		// avoid false negatives. The precise filtering happens at event level
		// in the block reader.
		// Exception: for MatchEqual, if the block's single label value doesn't
		// match, we still include the block because other events may have
		// different values for this key. The block label map stores one sample
		// value, not all values. So we always include when key exists.
		_ = val // label exists, conservatively include
	}
	return true
}
