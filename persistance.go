package bitstats

import (
	"encoding/json"

	"github.com/pkg/errors"
)

type jsonStatsRepr struct {
	Partitions map[string][]*jsonEventsRepr
}
type jsonEventsRepr struct {
	Name  string
	Value []uint64 // not using roaring bitmap byte repr for human consumption
}

func (s *Stats) Serialize() ([]byte, error) {
	ps := make(map[string][]*jsonEventsRepr, s.partitions.Len())
	s.partitions.Walk(func(parts []*statsPartition) bool {
		for _, part := range parts {
			evts := make([]*jsonEventsRepr, 0, part.events.Len())
			part.events.Walk(func(events []*statsEvent) bool {
				for _, event := range events {
					evt := &jsonEventsRepr{
						Name:  string(event.name),
						Value: event.values.ToArray(),
					}
					evts = append(evts, evt)
				}
				return true
			})
			ps[string(part.name)] = evts
		}
		return true
	})
	repr := &jsonStatsRepr{
		Partitions: ps,
	}
	data, err := json.Marshal(repr)
	if err != nil {
		return nil, errors.Wrap(err, "unable to encode to json")
	}
	return data, nil
}

func (s *Stats) Deserialize(data []byte) error {
	obj := &jsonStatsRepr{
		Partitions: make(map[string][]*jsonEventsRepr),
	}
	err := json.Unmarshal(data, obj)
	if err != nil {
		return errors.Wrap(err, "unable to unmarshall json")
	}
	for partitionName, events := range obj.Partitions {
		for _, event := range events {
			for _, val := range event.Value {
				s.Add(partitionName, event.Name, val)
			}
		}
	}
	return nil
}
