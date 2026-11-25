package indexobj

import (
	"testing"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/stretchr/testify/require"

	"github.com/grafana/loki/v3/pkg/dataobj"
	"github.com/grafana/loki/v3/pkg/dataobj/logsobj"
	"github.com/grafana/loki/v3/pkg/dataobj/sections/postings"
	"github.com/grafana/loki/v3/pkg/dataobj/sections/streams"
)

func TestBloomDenyList(t *testing.T) {
	cfg := logsobj.BuilderBaseConfig{
		TargetPageSize:          2048,
		MaxPageRows:             10000,
		TargetObjectSize:        1 << 22,
		TargetSectionSize:       1 << 21,
		BufferSize:              2048 * 8,
		SectionStripeMergeLimit: 2,
		BloomDenyList:           []string{"observed_timestamp"},
	}

	bloomRows := func(t *testing.T, flush func() (rows []postings.Row)) map[string]postings.Row {
		t.Helper()
		byColumn := make(map[string]postings.Row)
		for _, row := range flush() {
			require.Equal(t, postings.KindBloom, row.Kind)
			byColumn[row.ColumnName] = row
		}
		return byColumn
	}
	matches := func(t *testing.T, row postings.Row, value string) bool {
		t.Helper()
		var filter bloom.BloomFilter
		require.NoError(t, filter.UnmarshalBinary(row.BloomFilter))
		return filter.Test([]byte(value))
	}
	realFilter := func(t *testing.T, section int64, column string, values ...string) []byte {
		t.Helper()
		b := postings.NewBuilder(nil, 0, 0, 1<<20)
		b.PrepareBloomColumn("/obj", section, column, 100, 0)
		for _, v := range values {
			require.NoError(t, b.ObserveBloomPosting(postings.BloomObservation{ObjectPath: "/obj", SectionIndex: section, ColumnName: column, Value: v, Timestamp: time.Unix(0, 1).UTC()}))
		}
		got, err := b.BloomBytes("/obj", section, column)
		require.NoError(t, err)
		return got
	}

	t.Run("builder writes a filter that matches every value for a denied column", func(t *testing.T) {
		b, err := NewBuilder(testTenant, cfg, nil, NewBuilderMetrics(nil))
		require.NoError(t, err)

		b.PrepareBloomColumn("/obj", 0, "observed_timestamp", 1000, 0)
		for i, v := range []string{"1700000000000000001", "1700000000000000002"} {
			require.NoError(t, b.ObserveBloomPosting(postings.BloomObservation{ObjectPath: "/obj", SectionIndex: 0, ColumnName: "observed_timestamp", Value: v, StreamID: int64(i), Timestamp: time.Unix(1, 0)}))
		}

		rows := bloomRows(t, func() []postings.Row { return flushBuilderRows(t, b) })
		row := rows["observed_timestamp"]
		require.Equal(t, postings.PassAllBloomFilter(), row.BloomFilter)
		require.True(t, matches(t, row, "1700000000000000003"))
		require.Equal(t, []byte{0b00000011}, row.StreamIDBitmap)
	})

	t.Run("builder keeps a pruning filter for a column that is not denied", func(t *testing.T) {
		b, err := NewBuilder(testTenant, cfg, nil, NewBuilderMetrics(nil))
		require.NoError(t, err)

		b.PrepareBloomColumn("/obj", 0, "trace_id", 100, 0)
		require.NoError(t, b.ObserveBloomPosting(postings.BloomObservation{ObjectPath: "/obj", SectionIndex: 0, ColumnName: "trace_id", Value: "abc", Timestamp: time.Unix(1, 0)}))

		row := bloomRows(t, func() []postings.Row { return flushBuilderRows(t, b) })["trace_id"]
		require.True(t, matches(t, row, "abc"))
		require.False(t, matches(t, row, "def"))
	})

	t.Run("builder with an empty deny list keeps a pruning filter for every column", func(t *testing.T) {
		open := cfg
		open.BloomDenyList = nil
		b, err := NewBuilder(testTenant, open, nil, NewBuilderMetrics(nil))
		require.NoError(t, err)

		b.PrepareBloomColumn("/obj", 0, "observed_timestamp", 100, 0)
		require.NoError(t, b.ObserveBloomPosting(postings.BloomObservation{ObjectPath: "/obj", SectionIndex: 0, ColumnName: "observed_timestamp", Value: "1", Timestamp: time.Unix(1, 0)}))

		row := bloomRows(t, func() []postings.Row { return flushBuilderRows(t, b) })["observed_timestamp"]
		require.False(t, matches(t, row, "2"))
	})

	t.Run("merge builder gives a denied column that merges real and pass-all filters a filter that never prunes", func(t *testing.T) {
		b, err := NewMergeBuilder(cfg, nil, "tenant-1", streams.Layout{})
		require.NoError(t, err)

		// Section 0 comes from an old index with a real filter. Section 1
		// comes from a new index that already has the pass-all filter.
		require.NoError(t, b.AppendPostingsBloomEntry("tenant-1", postings.BloomEntry{
			ObjectPath: "/obj", SectionIndex: 0, ColumnName: "observed_timestamp",
			BloomFilter: realFilter(t, 0, "observed_timestamp", "1"), StreamIDBitmap: []byte{0x01},
		}))
		require.NoError(t, b.AppendPostingsBloomEntry("tenant-1", postings.BloomEntry{
			ObjectPath: "/obj", SectionIndex: 1, ColumnName: "observed_timestamp",
			BloomFilter: postings.PassAllBloomFilter(), StreamIDBitmap: []byte{0x01},
		}))
		require.NoError(t, b.AppendPostingsBloomEntry("tenant-1", postings.BloomEntry{
			ObjectPath: "/obj", SectionIndex: 0, ColumnName: "trace_id",
			BloomFilter: realFilter(t, 0, "trace_id", "abc"), StreamIDBitmap: []byte{0x01},
		}))

		rows := flushMergeBuilderRows(t, b)
		require.Len(t, rows, 3)
		for _, row := range rows {
			switch row.ColumnName {
			case "observed_timestamp":
				require.Equal(t, postings.PassAllBloomFilter(), row.BloomFilter, "section %d", row.SectionIndex)
				require.True(t, matches(t, row, "2"), "section %d", row.SectionIndex)
				require.Equal(t, []byte{0x01}, row.StreamIDBitmap)
			case "trace_id":
				require.True(t, matches(t, row, "abc"))
				require.False(t, matches(t, row, "def"))
			}
		}
	})

	t.Run("merge builder with an empty deny list copies real filters unchanged", func(t *testing.T) {
		open := cfg
		open.BloomDenyList = nil
		b, err := NewMergeBuilder(open, nil, "tenant-1", streams.Layout{})
		require.NoError(t, err)

		filter := realFilter(t, 0, "observed_timestamp", "1")
		require.NoError(t, b.AppendPostingsBloomEntry("tenant-1", postings.BloomEntry{
			ObjectPath: "/obj", SectionIndex: 0, ColumnName: "observed_timestamp",
			BloomFilter: filter, StreamIDBitmap: []byte{0x01},
		}))

		rows := flushMergeBuilderRows(t, b)
		require.Len(t, rows, 1)
		require.Equal(t, filter, rows[0].BloomFilter)
	})
}

// flushBuilderRows flushes b and returns the postings rows of its object.
func flushBuilderRows(t *testing.T, b *Builder) []postings.Row {
	t.Helper()
	obj, closer, err := b.Flush()
	require.NoError(t, err)
	t.Cleanup(func() { _ = closer.Close() })
	return postingsRowsOf(t, obj.Sections())
}

// flushMergeBuilderRows flushes b and returns the postings rows of its object.
func flushMergeBuilderRows(t *testing.T, b *MergeBuilder) []postings.Row {
	t.Helper()
	obj, closer, err := b.Flush()
	require.NoError(t, err)
	t.Cleanup(func() { _ = closer.Close() })
	return postingsRowsOf(t, obj.Sections())
}

// postingsRowsOf returns the rows of every postings section in sections.
func postingsRowsOf(t *testing.T, sections dataobj.Sections) []postings.Row {
	t.Helper()
	var rows []postings.Row
	for _, sec := range sections.Filter(postings.CheckSection) {
		section, err := postings.Open(t.Context(), sec)
		require.NoError(t, err)
		rows = append(rows, readAllPostingsRows(t.Context(), t, section)...)
	}
	return rows
}
