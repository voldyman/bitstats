package bitstats

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAddEvents(t *testing.T) {
	stats := New()
	stats.Add("partition", "new_message", 1)
	vals, ok := stats.Values("partition", "new_message")
	assert.True(t, ok, "expect added partition/event to exist")
	assert.ElementsMatch(t, vals, []uint64{1}, "expected elements match")
}

func TestListPartitions(t *testing.T) {
	stats := New()
	stats.Add("partition", "new_message", 1)
	vals := stats.Partitions()
	assert.ElementsMatch(t, vals, []string{"partition"}, "expected elements match")
}

func TestEventsList(t *testing.T) {
	stats := New()
	stats.Add("partition", "new_message", 1)
	stats.Add("partition", "new_message2", 2)
	stats.Add("partition2", "new_message3", 2)
	vals, ok := stats.Events("partition")
	assert.True(t, ok, "expect added partition/event to exist")
	assert.ElementsMatch(t, vals, []string{"new_message", "new_message2"}, "expected events to match")
}

func TestEventsByPrefix(t *testing.T) {
	stats := New()
	stats.Add("partition", "msg:1", 1)
	stats.Add("partition", "msg:2", 2)
	stats.Add("partition", "msg:3", 2)
	stats.Add("partition", "tango:1", 2)
	vals, ok := stats.EventsByPrefix("partition", "msg:")
	assert.True(t, ok, "expect added partition/event to exist")
	assert.ElementsMatch(t, vals, []string{"msg:1", "msg:2", "msg:3"}, "expected events to match")
}

func TestMarshaling(t *testing.T) {
	stats := New()
	stats.Add("partition", "msg:1", 1)
	stats.Add("partition", "msg:1", 2)
	stats.Add("partition", "msg:2", 2)
	stats.Add("partition", "msg:3", 2)
	stats.Add("partition2", "tango:1", 1)
	stats.Add("partition2", "tango:2", 2)
	data, err := stats.Serialize()
	assert.Nil(t, err, "serialization error should be nil")
	assert.NotEmpty(t, data, "serialized data should not be empty")

	jsonData, err := json.Marshal(stats)
	assert.Nil(t, err, "json marshing error should be nil")
	assert.NotEmpty(t, jsonData, "json data should not be empty")

	checkDeserializedStats := func(t *testing.T, newStats *Stats) {
		assert.Equal(t, []string{"partition", "partition2"}, newStats.Partitions(), "partitions should be deserialized")

		partition1Events, ok := newStats.Events("partition")
		assert.True(t, ok, "partition events shoud be retrieved")
		assert.Equal(t, []string{"msg:1", "msg:2", "msg:3"}, partition1Events, "partition events should be deserialized")

		eventsVals := map[string][]uint64{
			"msg:1": {1, 2},
			"msg:2": {2},
			"msg:3": {2},
		}
		for name, expectedVals := range eventsVals {
			vals, ok := newStats.Values("partition", name)
			assert.Truef(t, ok, "partition/%s values shoud be retrieved", name)
			assert.Equal(t, expectedVals, vals, "partition/%s values should match", name)
		}

		partition2Events, ok := newStats.Events("partition2")
		assert.True(t, ok, "partition2 events shoud be retrieved")
		assert.Equal(t, []string{"tango:1", "tango:2"}, partition2Events, "partition events should be deserialized")

		eventsVals = map[string][]uint64{
			"tango:1": {1},
			"tango:2": {2},
		}
		for name, expectedVals := range eventsVals {
			vals, ok := newStats.Values("partition2", name)
			assert.Truef(t, ok, "partition2/%s values shoud be retrieved", name)
			assert.Equal(t, expectedVals, vals, "partition2/%s values should match", name)
		}
	}
	t.Run("default deserialization", func(t *testing.T) {
		newStats := New()
		err = newStats.Deserialize(data)
		assert.Nil(t, err, "deserialization error should be nil")
		t.Log(string(data))
		checkDeserializedStats(t, newStats)

	})

	t.Run("json deserialization", func(t *testing.T) {
		jsonStats := New()
		err = json.Unmarshal(jsonData, jsonStats)
		assert.Nil(t, err, "json unmarshalling error should be nil")
		checkDeserializedStats(t, jsonStats)
	})
}

func TestRemoveMinPartition(t *testing.T) {
	s := New()
	s.Add("2022-01-01", "test", 1)
	s.Add("2022-01-02", "test", 1)
	s.Add("2022-01-03", "test", 1)
	assert.Equal(t, 3, s.PartitionsCount())

	rmName, ok := s.RemoveMinPartition()
	assert.True(t, ok)
	assert.Equal(t, "2022-01-01", rmName)

	assert.Equal(t, 2, s.PartitionsCount())
	assert.Equal(t, []string{"2022-01-02", "2022-01-03"}, s.Partitions(), "oldest date should be removed")

	for s.PartitionsCount() > 0 {
		_, ok := s.RemoveMinPartition()
		assert.True(t, ok)
	}
	assert.Equal(t, 0, s.PartitionsCount())

}
