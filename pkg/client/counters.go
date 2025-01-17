package client

import (
	"context"
	"fmt"
	"sync"

	p4_v1 "github.com/p4lang/p4runtime/go/p4/v1"
)

const (
	counterWildcardReadChSize = 100
)

func (c *Client) ModifyCounterEntry(ctx context.Context, counter string, index int64, data *p4_v1.CounterData) error {
	counterID := c.counterId(counter)
	entry := &p4_v1.CounterEntry{
		CounterId: counterID,
		Index:     &p4_v1.Index{Index: index},
		Data:      data,
	}
	update := &p4_v1.Update{
		Type: p4_v1.Update_MODIFY,
		Entity: &p4_v1.Entity{
			Entity: &p4_v1.Entity_CounterEntry{CounterEntry: entry},
		},
	}
	return c.WriteUpdate(ctx, update)
}

// 可以一次性更新一个Counter的多个Index为相同数据,主要用于重置计数器为0的场景
func (c *Client) ModifyManyCounterEntry(ctx context.Context, counter string, indexs []int64, data *p4_v1.CounterData) error {
	counterID := c.counterId(counter)
	updates := []*p4_v1.Update{}
	for _, index := range indexs {
		entry := &p4_v1.CounterEntry{
			CounterId: counterID,
			Index:     &p4_v1.Index{Index: index},
			Data:      data,
		}
		update := &p4_v1.Update{
			Type: p4_v1.Update_MODIFY,
			Entity: &p4_v1.Entity{
				Entity: &p4_v1.Entity_CounterEntry{CounterEntry: entry},
			},
		}
		updates = append(updates, update)
	}

	return c.WriteManyUpdate(ctx, updates)
}

func (c *Client) ReadCounterEntry(ctx context.Context, counter string, index int64) (*p4_v1.CounterData, error) {
	counterID := c.counterId(counter)
	entry := &p4_v1.CounterEntry{
		CounterId: counterID,
		Index:     &p4_v1.Index{Index: index},
	}
	readEntity, err := c.ReadEntitySingle(ctx, &p4_v1.Entity{
		Entity: &p4_v1.Entity_CounterEntry{CounterEntry: entry},
	})
	if err != nil {
		// 原样返回err,以便后续可以以GRPC的错误进行处理
		// return nil, fmt.Errorf("error when reading counter entry: %v", err)
		return nil, err
	}
	readEntry := readEntity.GetCounterEntry()
	if readEntry == nil {
		return nil, fmt.Errorf("server returned an entity but it is not a counter entry! ")
	}
	return readEntry.Data, nil
}

func (c *Client) ReadCounterEntryWildcard(ctx context.Context, counter string) ([]*p4_v1.CounterData, error) {
	p4Counter := c.findCounter(counter)
	entry := &p4_v1.CounterEntry{
		CounterId: p4Counter.Preamble.Id,
	}
	out := make([]*p4_v1.CounterData, 0, p4Counter.Size)
	readEntityCh := make(chan *p4_v1.Entity, counterWildcardReadChSize)
	var wg sync.WaitGroup
	var err error
	wg.Add(1)
	go func() {
		defer wg.Done()
		for readEntity := range readEntityCh {
			readEntry := readEntity.GetCounterEntry()
			if readEntry != nil {
				out = append(out, readEntry.Data)
			} else if err == nil {
				// only set the error if this is the first error we encounter
				// dp not stop reading from the channel, as doing so would cause
				// ReadEntityWildcard to block indefinitely
				err = fmt.Errorf("server returned an entity which is not a counter entry!")
			}
		}
	}()
	if err := c.ReadEntityWildcard(ctx, &p4_v1.Entity{
		Entity: &p4_v1.Entity_CounterEntry{CounterEntry: entry},
	}, readEntityCh); err != nil {
		// 原样返回err,以便后续可以以GRPC的错误进行处理
		// return nil, fmt.Errorf("error when reading counter entries: %v", err)
		return nil, err
	}
	wg.Wait()
	if err != nil {
		return nil, err
	}
	return out, nil
}
