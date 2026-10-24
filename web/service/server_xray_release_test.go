package service

import (
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/mhsanaei/3x-ui/v2/logger"
	"github.com/op/go-logging"
)

func serveFakeGitHub(t *testing.T, handler http.HandlerFunc) {
	t.Helper()
	serviceTestLoggerOnce.Do(func() { logger.InitLogger(logging.ERROR) })
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	prevAPI, prevWeb := githubAPIBaseURL, githubBaseURL
	githubAPIBaseURL, githubBaseURL = srv.URL, srv.URL
	t.Cleanup(func() { githubAPIBaseURL, githubBaseURL = prevAPI, prevWeb })
}

// Fork and upstream both publish v26.10.21, so each entry must carry the repo it installs from.
func TestGetXrayVersionsListsForkThenUpstream(t *testing.T) {
	const forkReleases = `[{"tag_name":"v26.10.21"}]`
	const upstreamReleases = `[{"tag_name":"v26.10.21"},{"tag_name":"v26.9.30"},{"tag_name":"v26.3.1"}]`
	cases := []struct {
		name         string
		upstreamDown bool
		want         []XrayRelease
	}{
		{
			name: "both repos",
			want: []XrayRelease{
				{Version: "v26.10.21", Repo: "ariansharifi/Xray-core"},
				{Version: "v26.10.21", Repo: "XTLS/Xray-core"},
				{Version: "v26.9.30", Repo: "XTLS/Xray-core"},
			},
		},
		{
			name:         "upstream rate-limited",
			upstreamDown: true,
			want:         []XrayRelease{{Version: "v26.10.21", Repo: "ariansharifi/Xray-core"}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			serveFakeGitHub(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/repos/ariansharifi/Xray-core/releases":
					w.Write([]byte(forkReleases))
				case "/repos/XTLS/Xray-core/releases":
					if tc.upstreamDown {
						w.WriteHeader(http.StatusForbidden)
						w.Write([]byte(`{"message":"API rate limit exceeded"}`))
						return
					}
					w.Write([]byte(upstreamReleases))
				default:
					http.NotFound(w, r)
				}
			})

			got, err := (&ServerService{}).GetXrayVersions()
			if err != nil {
				t.Fatalf("GetXrayVersions: %v", err)
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("GetXrayVersions = %v, want %v", got, tc.want)
			}
		})
	}
}

// Fork tags need not match the core version (v26.10.24 ships core 26.10.21), so the fork's build field names the release.
func TestInstalledXrayReleaseFromVersionOutput(t *testing.T) {
	cases := []struct {
		name, version, build string
		want                 XrayRelease
	}{
		{name: "fork release", version: "26.10.21", build: "v26.10.24-ariansharifi", want: XrayRelease{Version: "v26.10.24", Repo: "ariansharifi/Xray-core"}},
		{name: "fork build outside a release", version: "26.10.21", build: "4f65bc97-ariansharifi", want: XrayRelease{Repo: "ariansharifi/Xray-core"}},
		{name: "upstream", version: "26.10.21", build: "1b1647c", want: XrayRelease{Version: "v26.10.21", Repo: "XTLS/Xray-core"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := installedXrayRelease(tc.version, tc.build); got != tc.want {
				t.Errorf("installedXrayRelease(%q, %q) = %v, want %v", tc.version, tc.build, got, tc.want)
			}
		})
	}
}

func TestDownloadXRayFetchesFromTheChosenRepo(t *testing.T) {
	cases := []struct {
		name, version, repo string
		wantBody            string
		wantErr             string
	}{
		{name: "upstream", version: "v26.9.30", repo: "XTLS/Xray-core", wantBody: "upstream v26.9.30"},
		{name: "no repo means the fork", version: "v26.10.21", repo: "", wantBody: "fork v26.10.21"},
		{name: "version missing from repo", version: "v26.9.30", repo: "ariansharifi/Xray-core", wantErr: "404"},
		{name: "repo outside the allowlist", version: "v26.10.21", repo: "evil/Xray-core", wantErr: "not an allowed Xray release repo"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Chdir(t.TempDir())
			var requests atomic.Int32
			serveFakeGitHub(t, func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				switch {
				case strings.HasPrefix(r.URL.Path, "/XTLS/Xray-core/releases/download/v26.9.30/"):
					w.Write([]byte("upstream v26.9.30"))
				case strings.HasPrefix(r.URL.Path, "/ariansharifi/Xray-core/releases/download/v26.10.21/"):
					w.Write([]byte("fork v26.10.21"))
				default:
					http.NotFound(w, r)
				}
			})

			fileName, err := (&ServerService{}).downloadXRay(tc.version, tc.repo)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("downloadXRay error = %v, want one containing %q", err, tc.wantErr)
				}
				if tc.repo == "evil/Xray-core" && requests.Load() != 0 {
					t.Errorf("made %d request(s) to a repo outside the allowlist", requests.Load())
				}
				return
			}
			if err != nil {
				t.Fatalf("downloadXRay: %v", err)
			}
			body, err := os.ReadFile(fileName)
			if err != nil {
				t.Fatal(err)
			}
			if string(body) != tc.wantBody {
				t.Errorf("downloaded %q, want %q", body, tc.wantBody)
			}
		})
	}
}
