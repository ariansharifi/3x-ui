package sub

import (
	"sync"

	"github.com/goccy/go-json"

	"github.com/mhsanaei/3x-ui/v2/database"
	"github.com/mhsanaei/3x-ui/v2/database/model"
	"github.com/mhsanaei/3x-ui/v2/logger"
)

// subProtocols are the inbound protocols a subscription links to. "hysteria2" is allowed so imports
// stored with the literal v2 protocol string still surface (#4081).
var subProtocols = []string{"vmess", "vless", "trojan", "shadowsocks", "hysteria", "hysteria2"}

// subIndex maps each subId to the clients that carry it. Serving one subscription used to scan every
// client in SQL, load every client's stats row and decode every client, so each request cost as much as
// the whole panel and the request rate grew with the panel too. The index is rebuilt only when
// database.InboundsVersion moves; stats rows are always read live.
type subIndex struct {
	epoch, version int64
	inbounds       map[int]*indexedInbound
	bySubID        map[string][]subClient // in inbound id order, then settings order
}

type indexedInbound struct {
	inbound model.Inbound              // Settings and ClientStats left empty
	fields  map[string]json.RawMessage // settings without "clients"
}

type subClient struct {
	inboundID int
	email     string
	raw       json.RawMessage
}

var (
	subIndexMu  sync.Mutex
	subIndexCur *subIndex
)

// currentSubIndex returns an index at least as new as the database. The epoch and version are read
// before the inbounds, so a write racing the build only makes the index rebuild early, never go stale.
func currentSubIndex() (*subIndex, error) {
	epoch := database.Epoch()
	version, err := database.InboundsVersion()
	if err != nil {
		return nil, err
	}
	subIndexMu.Lock()
	defer subIndexMu.Unlock()
	if cur := subIndexCur; cur != nil && cur.epoch == epoch && cur.version >= version {
		return cur, nil
	}
	idx, err := buildSubIndex(epoch, version)
	if err != nil {
		return nil, err
	}
	subIndexCur = idx
	return idx, nil
}

func buildSubIndex(epoch, version int64) (*subIndex, error) {
	var inbounds []*model.Inbound
	err := database.GetDB().Model(model.Inbound{}).
		Where("protocol IN ? AND enable = ?", subProtocols, true).
		Order("id").Find(&inbounds).Error
	if err != nil {
		return nil, err
	}
	idx := &subIndex{epoch: epoch, version: version, inbounds: make(map[int]*indexedInbound, len(inbounds)), bySubID: make(map[string][]subClient)}
	for _, ib := range inbounds {
		var fields map[string]json.RawMessage
		var clients []json.RawMessage
		if err := json.Unmarshal([]byte(ib.Settings), &fields); err != nil {
			logger.Warning("subscription index: settings of inbound", ib.Id, "do not parse:", err)
			continue
		}
		if raw, ok := fields["clients"]; ok {
			if err := json.Unmarshal(raw, &clients); err != nil {
				logger.Warning("subscription index: clients of inbound", ib.Id, "do not parse:", err)
				continue
			}
			delete(fields, "clients")
		}
		entry := &indexedInbound{inbound: *ib, fields: fields}
		entry.inbound.Settings = ""
		idx.inbounds[ib.Id] = entry
		for _, raw := range clients {
			var key struct {
				SubID string `json:"subId"`
				Email string `json:"email"`
			}
			if json.Unmarshal(raw, &key) != nil || key.SubID == "" {
				continue
			}
			idx.bySubID[key.SubID] = append(idx.bySubID[key.SubID], subClient{inboundID: ib.Id, email: key.Email, raw: raw})
		}
	}
	return idx, nil
}

// withClients returns a fresh copy of the inbound that holds only these clients and their stats rows,
// so the link builders downstream decode a few clients instead of all of them. Callers may change it.
func (e *indexedInbound) withClients(clients []subClient) (*model.Inbound, error) {
	ib := e.inbound
	raws := make([]json.RawMessage, len(clients))
	emails := make([]string, 0, len(clients))
	for i, c := range clients {
		raws[i] = c.raw
		if c.email != "" {
			emails = append(emails, c.email)
		}
	}
	fields := make(map[string]json.RawMessage, len(e.fields)+1)
	for k, v := range e.fields {
		fields[k] = v
	}
	var err error
	if fields["clients"], err = json.Marshal(raws); err != nil {
		return nil, err
	}
	settings, err := json.Marshal(fields)
	if err != nil {
		return nil, err
	}
	ib.Settings = string(settings)
	if len(emails) > 0 {
		if err := database.GetDB().Where("inbound_id = ? AND email IN ?", ib.Id, emails).Find(&ib.ClientStats).Error; err != nil {
			return nil, err
		}
	}
	return &ib, nil
}
