package sub

import (
	"fmt"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/goccy/go-json"
	"github.com/op/go-logging"

	"github.com/mhsanaei/3x-ui/v2/database"
	"github.com/mhsanaei/3x-ui/v2/database/model"
	"github.com/mhsanaei/3x-ui/v2/logger"
	"github.com/mhsanaei/3x-ui/v2/web/service"
	"github.com/mhsanaei/3x-ui/v2/xray"
)

var subTestLoggerOnce sync.Once

func setupSubTestDB(t *testing.T) {
	t.Helper()
	subTestLoggerOnce.Do(func() { logger.InitLogger(logging.ERROR) })
	dir := t.TempDir()
	t.Setenv("XUI_DB_FOLDER", dir)
	t.Setenv("XUI_LOG_FOLDER", dir)
	if err := database.InitDB(filepath.Join(dir, "x-ui.db")); err != nil {
		t.Fatalf("InitDB: %v", err)
	}
	t.Cleanup(func() { database.CloseDB() })
}

func seedSubInbound(t *testing.T, port int, protocol model.Protocol, enable bool, clients ...model.Client) *model.Inbound {
	t.Helper()
	settings, err := json.Marshal(map[string]any{"clients": clients, "decryption": "none", "fallbacks": []any{}})
	if err != nil {
		t.Fatal(err)
	}
	inbound := &model.Inbound{UserId: 1, Port: port, Protocol: protocol, Tag: fmt.Sprintf("inbound-%d", port), Enable: enable,
		Remark: fmt.Sprintf("r%d", port), Settings: string(settings),
		StreamSettings: `{"network":"ws","security":"none","wsSettings":{"path":"/p"}}`, Sniffing: `{}`}
	if err := database.GetDB().Create(inbound).Error; err != nil {
		t.Fatal(err)
	}
	for i, c := range clients {
		if c.Email == "" {
			continue
		}
		stat := &xray.ClientTraffic{InboundId: inbound.Id, Enable: c.Enable, Email: c.Email, Up: int64(port + i), Down: int64(2 * (port + i))}
		if err := database.GetDB().Create(stat).Error; err != nil {
			t.Fatal(err)
		}
	}
	return inbound
}

// legacyInboundsBySubId is the lookup subIndex replaced: whole inbounds with every stats row.
func legacyInboundsBySubId(t *testing.T, subId string) []*model.Inbound {
	t.Helper()
	var inbounds []*model.Inbound
	err := database.GetDB().Model(model.Inbound{}).Preload("ClientStats").Where(`id in (
		SELECT DISTINCT inbounds.id
		FROM inbounds,
			JSON_EACH(JSON_EXTRACT(inbounds.settings, '$.clients')) AS client
		WHERE
			protocol in ('vmess','vless','trojan','shadowsocks','hysteria','hysteria2')
			AND JSON_EXTRACT(client.value, '$.subId') = ? AND enable = ?
	)`, subId, true).Find(&inbounds).Error
	if err != nil {
		t.Fatal(err)
	}
	return inbounds
}

func settingsWithoutClients(t *testing.T, settings string) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(settings), &m); err != nil {
		t.Fatal(err)
	}
	delete(m, "clients")
	return m
}

func newClient(email, subID string, enable bool) model.Client {
	return model.Client{ID: "id-" + email, Email: email, SubID: subID, Enable: enable, ExpiryTime: 1800000000000, TotalGB: 5 << 30}
}

// Every link builder runs on what getInboundsBySubId returns, so the trimmed inbounds must carry exactly
// what the old lookup's whole inbounds offered for that subId: same inbounds and order, same clients,
// same other settings and the same stats rows.
func TestSubIndexMatchesLegacyLookup(t *testing.T) {
	setupSubTestDB(t)
	seedSubInbound(t, 45001, model.VLESS, true,
		newClient("a", "s1", true), newClient("b", "s2", true), newClient("c", "s1", true),
		newClient("d", "s3", false), newClient("e", "", true))
	seedSubInbound(t, 45002, model.VMESS, true, newClient("f", "s1", true), newClient("g", "s4", true))
	seedSubInbound(t, 45003, model.Trojan, false, newClient("h", "s1", true))
	seedSubInbound(t, 45004, model.Protocol("wireguard"), true, newClient("i", "s1", true))

	var inboundService service.InboundService
	s := &SubService{}
	for _, subID := range []string{"s1", "s2", "s3", "s4", "missing"} {
		want := legacyInboundsBySubId(t, subID)
		got, err := s.getInboundsBySubId(subID)
		if err != nil {
			t.Fatalf("%s: %v", subID, err)
		}
		if len(got) != len(want) {
			t.Fatalf("%s: %d inbounds, want %d", subID, len(got), len(want))
		}
		for i := range want {
			w, g := want[i], got[i]
			if g.Id != w.Id || g.Listen != w.Listen || g.Port != w.Port || g.Protocol != w.Protocol || g.Tag != w.Tag ||
				g.Remark != w.Remark || g.Enable != w.Enable || g.StreamSettings != w.StreamSettings || g.ExpiryTime != w.ExpiryTime {
				t.Errorf("%s: inbound %d fields differ", subID, w.Id)
			}
			if !reflect.DeepEqual(settingsWithoutClients(t, g.Settings), settingsWithoutClients(t, w.Settings)) {
				t.Errorf("%s: inbound %d settings outside clients differ", subID, w.Id)
			}
			wantClients, _ := inboundService.GetClients(w)
			var matched []model.Client
			for _, c := range wantClients {
				if c.SubID == subID {
					matched = append(matched, c)
				}
			}
			gotClients, _ := inboundService.GetClients(g)
			if !reflect.DeepEqual(gotClients, matched) {
				t.Errorf("%s: inbound %d clients = %+v, want %+v", subID, w.Id, gotClients, matched)
			}
			for _, c := range matched {
				if gs, ws := s.getClientTraffics(g.ClientStats, c.Email), s.getClientTraffics(w.ClientStats, c.Email); !reflect.DeepEqual(gs, ws) {
					t.Errorf("%s: stats of %s = %+v, want %+v", subID, c.Email, gs, ws)
				}
			}
		}
	}

	links, _, _, err := (&SubService{remarkModel: "-ieo"}).GetSubs("s1", "example.com")
	if err != nil || len(links) != 3 {
		t.Errorf("GetSubs(s1) = %d links, %v; want 3 links (a, c, f)", len(links), err)
	}
}

// A cached index is only safe if every edit reaches it, whoever writes; traffic ticks must not rebuild it,
// and an imported database must never be served from the old one's index.
func TestSubIndexFollowsEditsNotTrafficTicks(t *testing.T) {
	setupSubTestDB(t)
	inbound := seedSubInbound(t, 46001, model.VLESS, true, newClient("a", "s1", true), newClient("b", "s2", true))
	s := &SubService{}
	emails := func(subID string) []string {
		t.Helper()
		inbounds, err := s.getInboundsBySubId(subID)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, ib := range inbounds {
			var settings struct {
				Clients []model.Client `json:"clients"`
			}
			json.Unmarshal([]byte(ib.Settings), &settings)
			for _, c := range settings.Clients {
				out = append(out, c.Email)
			}
		}
		return out
	}
	if got := emails("s2"); !reflect.DeepEqual(got, []string{"b"}) {
		t.Fatalf("s2 = %v, want [b]", got)
	}
	built := subIndexCur

	db := database.GetDB()
	db.Model(&model.Inbound{}).Where("id = ?", inbound.Id).Updates(map[string]any{"up": 100, "down": 200})
	db.Model(&xray.ClientTraffic{}).Where("email = ?", "b").Update("up", 777)
	got, _ := s.getInboundsBySubId("s2")
	if subIndexCur != built {
		t.Error("a traffic tick rebuilt the index")
	}
	if len(got) != 1 || len(got[0].ClientStats) != 1 || got[0].ClientStats[0].Up != 777 {
		t.Errorf("stats are not read live: %+v", got)
	}

	// edit b's subId with raw SQL, the way a script or another process would
	db.Exec(`UPDATE inbounds SET settings = json_set(settings, '$.clients[1].subId', 's5') WHERE id = ?`, inbound.Id)
	if got := emails("s2"); len(got) != 0 {
		t.Errorf("after the edit s2 = %v, want none", got)
	}
	if got := emails("s5"); !reflect.DeepEqual(got, []string{"b"}) {
		t.Errorf("after the edit s5 = %v, want [b]", got)
	}

	db.Model(&model.Inbound{}).Where("id = ?", inbound.Id).Update("enable", false)
	if got := emails("s1"); len(got) != 0 {
		t.Errorf("disabled inbound still served: s1 = %v", got)
	}

	// an imported database: different content, its own counter
	setupSubTestDB(t)
	seedSubInbound(t, 46002, model.VLESS, true, newClient("z", "s1", true))
	if got := emails("s1"); !reflect.DeepEqual(got, []string{"z"}) {
		t.Errorf("after InitDB of another database s1 = %v, want [z]", got)
	}
}
