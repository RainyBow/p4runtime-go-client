package client

import (
	"context"
	"fmt"
	"sync"

	p4_v1 "github.com/p4lang/p4runtime/go/p4/v1"
)

var (
	multicastGroupWildcardReadChSize = 100
)

func (c *Client) InsertMulticastGroup(ctx context.Context, mgid uint32, ports []uint32) error {
	entry := &p4_v1.MulticastGroupEntry{
		MulticastGroupId: mgid,
	}
	for idx, port := range ports {
		replica := &p4_v1.Replica{
			EgressPort: port,
			Instance:   uint32(idx),
		}
		entry.Replicas = append(entry.Replicas, replica)
	}

	preEntry := &p4_v1.PacketReplicationEngineEntry{
		Type: &p4_v1.PacketReplicationEngineEntry_MulticastGroupEntry{
			MulticastGroupEntry: entry,
		},
	}

	updateType := p4_v1.Update_INSERT
	update := &p4_v1.Update{
		Type: updateType,
		Entity: &p4_v1.Entity{
			Entity: &p4_v1.Entity_PacketReplicationEngineEntry{
				PacketReplicationEngineEntry: preEntry,
			},
		},
	}

	return c.WriteUpdate(ctx, update)
}

// modify multicast group
func (c *Client) ModifyMulticastGroup(ctx context.Context, mgid uint32, ports []uint32) error {
	entry := &p4_v1.MulticastGroupEntry{
		MulticastGroupId: mgid,
	}
	for idx, port := range ports {
		replica := &p4_v1.Replica{
			EgressPort: port,
			Instance:   uint32(idx),
		}
		entry.Replicas = append(entry.Replicas, replica)
	}

	preEntry := &p4_v1.PacketReplicationEngineEntry{
		Type: &p4_v1.PacketReplicationEngineEntry_MulticastGroupEntry{
			MulticastGroupEntry: entry,
		},
	}

	updateType := p4_v1.Update_MODIFY
	update := &p4_v1.Update{
		Type: updateType,
		Entity: &p4_v1.Entity{
			Entity: &p4_v1.Entity_PacketReplicationEngineEntry{
				PacketReplicationEngineEntry: preEntry,
			},
		},
	}

	return c.WriteUpdate(ctx, update)
}

func (c *Client) DeleteMulticastGroup(ctx context.Context, mgid uint32) error {
	entry := &p4_v1.MulticastGroupEntry{
		MulticastGroupId: mgid,
	}

	preEntry := &p4_v1.PacketReplicationEngineEntry{
		Type: &p4_v1.PacketReplicationEngineEntry_MulticastGroupEntry{
			MulticastGroupEntry: entry,
		},
	}

	updateType := p4_v1.Update_DELETE
	update := &p4_v1.Update{
		Type: updateType,
		Entity: &p4_v1.Entity{
			Entity: &p4_v1.Entity_PacketReplicationEngineEntry{
				PacketReplicationEngineEntry: preEntry,
			},
		},
	}

	return c.WriteUpdate(ctx, update)
}

// read all mutlicast group entry
func (c *Client) ReadMulticastGroupWildcard(ctx context.Context) ([]*p4_v1.MulticastGroupEntry, error) {
	entry := &p4_v1.PacketReplicationEngineEntry{
		Type: &p4_v1.PacketReplicationEngineEntry_MulticastGroupEntry{},
	}

	out := make([]*p4_v1.MulticastGroupEntry, 0)
	readEntityCh := make(chan *p4_v1.Entity, multicastGroupWildcardReadChSize)

	var wg sync.WaitGroup
	var err error
	wg.Add(1)

	go func() {
		defer wg.Done()
		for readEntity := range readEntityCh {
			readEntry := readEntity.GetPacketReplicationEngineEntry()
			if readEntry != nil {
				out = append(out, readEntry.GetMulticastGroupEntry())
			} else if err == nil {
				// only set the error if this is the first error we encounter
				// dp not stop reading from the channel, as doing so would cause
				// ReadEntityWildcard to block indefinitely
				err = fmt.Errorf("server returned an entity which is not a mutlicastgroup entry!")
			}
		}
	}()

	if err := c.ReadEntityWildcard(ctx, &p4_v1.Entity{
		Entity: &p4_v1.Entity_PacketReplicationEngineEntry{
			PacketReplicationEngineEntry: entry,
		},
	}, readEntityCh); err != nil {
		return nil, fmt.Errorf("error when reading table entries: %v", err)
	}

	wg.Wait()
	if err != nil {
		return nil, err
	}
	return out, nil
}

// read one mutlicast group entry
func (c *Client) ReadMulticastGroup(ctx context.Context, mgid uint32) (*p4_v1.MulticastGroupEntry, error) {
	entry := &p4_v1.PacketReplicationEngineEntry{
		Type: &p4_v1.PacketReplicationEngineEntry_MulticastGroupEntry{
			MulticastGroupEntry: &p4_v1.MulticastGroupEntry{
				MulticastGroupId: mgid,
			},
		},
	}
	entity := &p4_v1.Entity{
		Entity: &p4_v1.Entity_PacketReplicationEngineEntry{PacketReplicationEngineEntry: entry},
	}
	readEntity, err := c.ReadEntitySingle(ctx, entity)
	if err != nil {
		return nil, fmt.Errorf("error when reading multicast group entry: %v", err)
	}

	readEntry := readEntity.GetPacketReplicationEngineEntry()
	if readEntry == nil {
		return nil, fmt.Errorf("server returned an entity but it is not a multicast group entry! ")
	}

	return readEntry.GetMulticastGroupEntry(), nil
}
