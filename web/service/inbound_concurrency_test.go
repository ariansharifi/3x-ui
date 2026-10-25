package service

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mhsanaei/3x-ui/v2/database"
	"github.com/mhsanaei/3x-ui/v2/database/model"
	"github.com/mhsanaei/3x-ui/v2/xray"
)

// Each client write rewrites the inbound's whole client list; overlapping ones used to save stale copies over each other.
func TestConcurrentClientWritesKeepEveryEdit(t *testing.T) {
	setupXrayAPITest(t)
	s := &InboundService{}
	const existing, writers = 2000, 20
	seed := make([]model.Client, existing)
	for i := range seed {
		seed[i] = model.Client{ID: uuid.NewString(), Email: fmt.Sprintf("seed-%d", i), Enable: true}
	}
	inbound := seedVlessInbound(t, s, 42001, seed...)

	stop := make(chan struct{})
	jobDone := make(chan struct{})
	go func() { // the traffic job rewrites the same row whenever it renews or disables a client
		defer close(jobDone)
		traffic := []*xray.ClientTraffic{{Email: "seed-0", Up: 1, Down: 1}}
		for {
			select {
			case <-stop:
				return
			default:
			}
			if _, _, err := s.AddTraffic(nil, traffic); err != nil {
				t.Errorf("AddTraffic: %v", err)
			}
		}
	}()

	var wg sync.WaitGroup
	for i := range writers {
		wg.Add(2)
		go func() {
			defer wg.Done()
			added := model.Client{ID: uuid.NewString(), Email: fmt.Sprintf("added-%d", i), Enable: true}
			if _, err := s.AddInboundClient(&model.Inbound{Id: inbound.Id, Settings: clientSettings(t, added)}); err != nil {
				t.Errorf("AddInboundClient %d: %v", i, err)
			}
		}()
		go func() {
			defer wg.Done()
			edited := seed[i+1]
			edited.Comment = fmt.Sprintf("edited-%d", i)
			if _, err := s.UpdateInboundClient(&model.Inbound{Id: inbound.Id, Settings: clientSettings(t, edited)}, edited.ID); err != nil {
				t.Errorf("UpdateInboundClient %d: %v", i, err)
			}
		}()
	}
	wg.Wait()
	close(stop)
	<-jobDone

	stored, err := s.GetInbound(inbound.Id)
	if err != nil {
		t.Fatal(err)
	}
	clients, err := s.GetClients(stored)
	if err != nil {
		t.Fatal(err)
	}
	byEmail := make(map[string]model.Client, len(clients))
	for _, c := range clients {
		byEmail[c.Email] = c
	}
	if len(clients) != existing+writers {
		t.Errorf("inbound holds %d clients, want %d", len(clients), existing+writers)
	}
	for i := range writers {
		if _, ok := byEmail[fmt.Sprintf("added-%d", i)]; !ok {
			t.Errorf("added-%d was dropped", i)
		}
		if got := byEmail[seed[i+1].Email].Comment; got != fmt.Sprintf("edited-%d", i) {
			t.Errorf("%s comment = %q, edit was dropped", seed[i+1].Email, got)
		}
	}
}

// With BEGIN IMMEDIATE the purge's transaction holds the write lock, so deleting the emptied inbound on another connection used to wait out the busy timeout and fail.
func TestDelDepletedClientsRemovesEmptiedInbound(t *testing.T) {
	setupXrayAPITest(t)
	s := &InboundService{}
	expired := model.Client{ID: uuid.NewString(), Email: "expired", Enable: true, ExpiryTime: time.Now().Add(-time.Hour).UnixMilli()}
	inbound := seedVlessInbound(t, s, 42002, expired)

	start := time.Now()
	if err := s.DelDepletedClients(inbound.Id); err != nil {
		t.Fatalf("DelDepletedClients: %v", err)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Errorf("DelDepletedClients took %v, it waited on its own lock", took)
	}
	var count int64
	database.GetDB().Model(model.Inbound{}).Where("id = ?", inbound.Id).Count(&count)
	if count != 0 {
		t.Error("inbound with no client left was not deleted")
	}
}
