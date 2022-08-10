package client

import (
	"context"
	"fmt"
	"sync"

	p4_v1 "github.com/p4lang/p4runtime/go/p4/v1"
)

var (
	cloneSessionWildcardReadChSize = 100
)

func (c *Client) InsertCloneSession(ctx context.Context, session_id uint32, packet_length int32, ports []uint32) error {
	entry := &p4_v1.CloneSessionEntry{
		SessionId:         session_id,
		PacketLengthBytes: packet_length,
	}
	for idx, port := range ports {
		replica := &p4_v1.Replica{
			EgressPort: port,
			Instance:   uint32(idx),
		}
		entry.Replicas = append(entry.Replicas, replica)
	}

	preEntry := &p4_v1.PacketReplicationEngineEntry{
		Type: &p4_v1.PacketReplicationEngineEntry_CloneSessionEntry{
			CloneSessionEntry: entry,
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
func (c *Client) ModifyCloneSession(ctx context.Context, session_id uint32, packet_length int32, ports []uint32) error {
	entry := &p4_v1.CloneSessionEntry{
		SessionId:         session_id,
		PacketLengthBytes: packet_length,
	}
	for idx, port := range ports {
		replica := &p4_v1.Replica{
			EgressPort: port,
			Instance:   uint32(idx),
		}
		entry.Replicas = append(entry.Replicas, replica)
	}

	preEntry := &p4_v1.PacketReplicationEngineEntry{
		Type: &p4_v1.PacketReplicationEngineEntry_CloneSessionEntry{
			CloneSessionEntry: entry,
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

func (c *Client) DeleteCloneSession(ctx context.Context, session_id uint32) error {
	entry := &p4_v1.CloneSessionEntry{
		SessionId: session_id,
	}

	preEntry := &p4_v1.PacketReplicationEngineEntry{
		Type: &p4_v1.PacketReplicationEngineEntry_CloneSessionEntry{
			CloneSessionEntry: entry,
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

// read all clone session entry
func (c *Client) ReadCloneSessionWildcard(ctx context.Context) ([]*p4_v1.CloneSessionEntry, error) {
	entry := &p4_v1.PacketReplicationEngineEntry{
		Type: &p4_v1.PacketReplicationEngineEntry_CloneSessionEntry{},
	}

	out := make([]*p4_v1.CloneSessionEntry, 0)
	readEntityCh := make(chan *p4_v1.Entity, cloneSessionWildcardReadChSize)

	var wg sync.WaitGroup
	var err error
	wg.Add(1)

	go func() {
		defer wg.Done()
		for readEntity := range readEntityCh {
			readEntry := readEntity.GetPacketReplicationEngineEntry()
			if readEntry != nil {
				out = append(out, readEntry.GetCloneSessionEntry())
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

// read one clone session entry
func (c *Client) ReadCloneSession(ctx context.Context, session_id uint32) (*p4_v1.CloneSessionEntry, error) {
	entry := &p4_v1.PacketReplicationEngineEntry{
		Type: &p4_v1.PacketReplicationEngineEntry_CloneSessionEntry{
			CloneSessionEntry: &p4_v1.CloneSessionEntry{
				SessionId: session_id,
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

	return readEntry.GetCloneSessionEntry(), nil
}
