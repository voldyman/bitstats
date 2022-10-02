package bitstats

import (
	"bytes"

	roaring "github.com/RoaringBitmap/roaring/roaring64"
	"github.com/tidwall/btree"
)

type Stats struct {
	partitions *btree.BTreeG[*statsPartition]
}

type statsPartition struct {
	name   []byte
	events *btree.BTreeG[*statsEvent]
}

type statsEvent struct {
	name   []byte
	values *roaring.Bitmap
}

func New() *Stats {
	return &Stats{
		partitions: btree.NewBTreeGOptions(
			statsPartitionLess,
			btree.Options{NoLocks: true},
		),
	}
}

// Add an event to the stats, returns
// true if a new event was added
// false if the event already existed
func (s *Stats) Add(partition, event string, value uint64) bool {
	queryPartition := &statsPartition{name: s2b(partition)}
	part, partitionExists := s.partitions.Get(queryPartition)
	if !partitionExists {
		part = queryPartition
		part.initialize()
		s.partitions.Set(part)
	}
	queryEvent := &statsEvent{name: s2b(event)}
	e, eventExists := part.events.Get(queryEvent)
	if !eventExists {
		e = queryEvent
		e.initialize()
		part.events.Set(e)
	}
	return eventExists || e.values.CheckedAdd(value)
}

// Remove an event from the stats, returns
// true if a new event was added
// false if the event already existed
func (s *Stats) Remove(partition, event string, value uint64) bool {
	queryPartition := &statsPartition{name: s2b(partition)}
	part, partitionExists := s.partitions.Get(queryPartition)
	if !partitionExists {
		return false
	}
	queryEvent := &statsEvent{name: s2b(event)}
	e, eventExists := part.events.Get(queryEvent)
	if !eventExists {
		return false
	}
	return e.values.CheckedRemove(value)
}

// Remove a partition from the stats, returns
// true if a partition was removed
// false if the partition doesn't exist
func (s *Stats) RemovePartition(partition string) bool {
	queryPartition := &statsPartition{name: s2b(partition)}
	_, removed := s.partitions.Delete(queryPartition)
	return removed
}

func (s *Stats) PartitionsCount() int {
	return s.partitions.Len()
}

func (s *Stats) EventsCount(partition string) (int, bool) {
	queryPartition := &statsPartition{name: s2b(partition)}
	part, partitionExists := s.partitions.Get(queryPartition)
	if !partitionExists {
		return 0, false
	}
	return part.events.Len(), true
}

func (s *Stats) ValuesCount(partition, event string) (int, bool) {
	p, ok := s.partitions.Get(&statsPartition{name: s2b(partition)})
	if !ok {
		return 0, false
	}
	e, ok := p.events.Get(&statsEvent{name: s2b(event)})
	if !ok {
		return 0, false
	}
	return int(e.values.GetCardinality()), true
}

// Values queries event values from a partition, returns
// <values>, true if the partion/event exists
// <nil>, false if the partition/event does not exist
func (s *Stats) Values(partition, event string) ([]uint64, bool) {
	p, ok := s.partitions.Get(&statsPartition{name: s2b(partition)})
	if !ok {
		return nil, false
	}
	e, ok := p.events.Get(&statsEvent{name: s2b(event)})
	if !ok {
		return nil, false
	}
	return e.values.ToArray(), true
}

// Values queries event values from a partition, returns
// <values>, true if the partion/event exists
// <nil>, false if the partition/event does not exist
func (s *Stats) ValuesSet(partition, event string) (*roaring.Bitmap, bool) {
	p, ok := s.partitions.Get(&statsPartition{name: s2b(partition)})
	if !ok {
		return nil, false
	}
	e, ok := p.events.Get(&statsEvent{name: s2b(event)})
	if !ok {
		return nil, false
	}
	result := e.values.Clone()
	result.RunOptimize()
	return result, true
}

// Events queries events from a partition, returns
// <events>, true if the partion exists
// <empty>, false if the partition does not exist
func (s *Stats) Events(partition string) ([]string, bool) {
	p, ok := s.partitions.Get(&statsPartition{name: s2b(partition)})
	if !ok {
		return nil, false
	}
	events := make([]string, 0, p.events.Len())
	p.events.Walk(func(items []*statsEvent) bool {
		for _, item := range items {
			events = append(events, string(item.name))
		}
		return true
	})
	return events, true
}

// EventsByPrefix queries events from a partition with the specified prefix, returns
// (<event names>, true) if the partition exists
// (<empty>, false) if the partition does not exist
func (s *Stats) EventsByPrefix(partition, eventPrefix string) ([]string, bool) {
	p, ok := s.partitions.Get(&statsPartition{name: s2b(partition)})
	if !ok {
		return nil, false
	}
	eventStartKey := s2b(eventPrefix)
	eventsStart := &statsEvent{name: eventStartKey}
	eventsEnd := &statsEvent{name: append(eventStartKey, 255)}

	result := []string{}
	p.events.Ascend(eventsStart, func(item *statsEvent) bool {
		if !p.events.Less(item, eventsEnd) {
			return false
		}
		result = append(result, string(item.name))
		return true
	})
	return result, true
}

// Partitions queries partitions, returns a list of partitions
func (s *Stats) Partitions() []string {
	l := s.partitions.Len()
	result := make([]string, 0, l)
	s.partitions.Walk(func(items []*statsPartition) bool {
		for _, item := range items {
			result = append(result, string(item.name))
		}
		return true
	})
	return result
}

func statsPartitionLess(a, b *statsPartition) bool {
	return bytes.Compare(a.name, b.name) < 0
}

func (p *statsPartition) initialize() {
	p.events = btree.NewBTreeGOptions(
		statsEventLess,
		btree.Options{NoLocks: true},
	)
}

func statsEventLess(a, b *statsEvent) bool {
	return bytes.Compare(a.name, b.name) < 0
}

func (e *statsEvent) initialize() {
	e.values = roaring.New()
}

func s2b(s string) []byte {
	return []byte(s)
}
