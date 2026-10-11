package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"ecobrio/internal/geospatial"
	"ecobrio/internal/incidents"
	ws "ecobrio/internal/websocket"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupIncidentWSTestServer(t *testing.T) (*httptest.Server, *incidents.Service, *ws.Hub) {
	ctx := context.Background()
	repo := incidents.NewMemoryRepository()
	geoIdx := geospatial.NewMemoryIndexer()

	hub := ws.NewHub()
	go hub.Run(ctx)

	svc := incidents.NewService(repo, geoIdx, hub)
	hub.SetSnapshotProvider(svc.GetActiveSnapshot)

	mux := http.NewServeMux()
	hub.RegisterRoutes(mux)

	handler := incidents.NewHandler(svc)
	handler.RegisterRoutes(mux)

	server := httptest.NewServer(mux)
	return server, svc, hub
}

func TestWebSocket_MultiClientRealtimeFanout(t *testing.T) {
	server, _, _ := setupIncidentWSTestServer(t)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/ws/incidents"

	// Connect Client 1
	conn1, _, err := websocket.DefaultDialer.Dial(wsURL+"?client_id=client-1", nil)
	require.NoError(t, err)
	defer conn1.Close()

	// Read initial SNAPSHOT_SYNC on client 1
	var sync1 ws.Event
	require.NoError(t, conn1.ReadJSON(&sync1))
	assert.Equal(t, ws.EventSnapshotSync, sync1.Type)

	// Connect Client 2
	conn2, _, err := websocket.DefaultDialer.Dial(wsURL+"?client_id=client-2", nil)
	require.NoError(t, err)
	defer conn2.Close()

	// Read initial SNAPSHOT_SYNC on client 2
	var sync2 ws.Event
	require.NoError(t, conn2.ReadJSON(&sync2))
	assert.Equal(t, ws.EventSnapshotSync, sync2.Type)

	// 1. Report an incident via HTTP POST /api/v1/incidents
	createReq := map[string]any{
		"description": "Hazardous lead battery breach on roadside",
		"stream_type": "HAZARDOUS",
		"severity":    "HIGH",
		"latitude":    28.6139,
		"longitude":   77.2090,
	}
	body, _ := json.Marshal(createReq)
	resp, err := http.Post(server.URL+"/api/v1/incidents", "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	var createdInc incidents.Incident
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&createdInc))
	_ = resp.Body.Close()

	// Assert BOTH Client 1 and Client 2 receive INCIDENT_CREATED simultaneously
	var ev1, ev2 ws.Event
	conn1.SetReadDeadline(time.Now().Add(2 * time.Second))
	conn2.SetReadDeadline(time.Now().Add(2 * time.Second))

	require.NoError(t, conn1.ReadJSON(&ev1))
	require.NoError(t, conn2.ReadJSON(&ev2))

	assert.Equal(t, ws.EventIncidentCreated, ev1.Type)
	assert.Equal(t, ws.EventIncidentCreated, ev2.Type)
	assert.Equal(t, ev1.Sequence, ev2.Sequence)

	// 2. Transition status: RAISED -> IN_PROGRESS via HTTP PATCH
	updateReq := map[string]any{
		"status": "IN_PROGRESS",
	}
	patchBody, _ := json.Marshal(updateReq)
	req, _ := http.NewRequest(http.MethodPatch, fmt.Sprintf("%s/api/v1/incidents/%s/status", server.URL, createdInc.ID), bytes.NewReader(patchBody))
	req.Header.Set("Content-Type", "application/json")
	patchResp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, patchResp.StatusCode)
	_ = patchResp.Body.Close()

	// Assert BOTH clients receive INCIDENT_UPDATED
	var updateEv1, updateEv2 ws.Event
	require.NoError(t, conn1.ReadJSON(&updateEv1))
	require.NoError(t, conn2.ReadJSON(&updateEv2))

	assert.Equal(t, ws.EventIncidentUpdated, updateEv1.Type)
	assert.Equal(t, ws.EventIncidentUpdated, updateEv2.Type)
	assert.Equal(t, updateEv1.Sequence, updateEv2.Sequence)
	assert.True(t, updateEv1.Sequence > ev1.Sequence)
}

func TestWebSocket_ReconnectGapResync(t *testing.T) {
	server, _, _ := setupIncidentWSTestServer(t)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/ws/incidents"

	// 1. Initial connect and create baseline incident to advance sequence
	conn, _, err := websocket.DefaultDialer.Dial(wsURL+"?client_id=reconnect-client&last_seq=0", nil)
	require.NoError(t, err)
	var initialSync ws.Event
	require.NoError(t, conn.ReadJSON(&initialSync))
	assert.Equal(t, ws.EventSnapshotSync, initialSync.Type)

	// Post baseline incident while connected
	baseReq := map[string]any{
		"description": "Baseline initial incident",
		"stream_type": "ORGANIC",
		"severity":    "LOW",
		"latitude":    28.5,
		"longitude":   77.1,
	}
	body, _ := json.Marshal(baseReq)
	resp, err := http.Post(server.URL+"/api/v1/incidents", "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	_ = resp.Body.Close()

	var baseEv ws.Event
	require.NoError(t, conn.ReadJSON(&baseEv))
	lastSeenSeq := baseEv.Sequence
	assert.True(t, lastSeenSeq > 0, "lastSeenSeq must be > 0")

	// Disconnect client
	_ = conn.Close()

	// 2. Post two incidents while client is disconnected
	for i := 1; i <= 2; i++ {
		createReq := map[string]any{
			"description": fmt.Sprintf("Plastic waste accumulation #%d", i),
			"stream_type": "PLASTICS",
			"severity":    "MEDIUM",
			"latitude":    28.6000 + float64(i)*0.01,
			"longitude":   77.2000 + float64(i)*0.01,
		}
		b, _ := json.Marshal(createReq)
		r, err := http.Post(server.URL+"/api/v1/incidents", "application/json", bytes.NewReader(b))
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, r.StatusCode)
		_ = r.Body.Close()
	}

	// 3. Reconnect with last_seq=lastSeenSeq
	reconnURL := fmt.Sprintf("%s?client_id=reconnect-client&last_seq=%d", wsURL, lastSeenSeq)
	connReconn, _, err := websocket.DefaultDialer.Dial(reconnURL, nil)
	require.NoError(t, err)
	defer connReconn.Close()

	// Should immediately receive the two missed events replayed from the ring buffer!
	connReconn.SetReadDeadline(time.Now().Add(2 * time.Second))
	var replayed1, replayed2 ws.Event
	require.NoError(t, connReconn.ReadJSON(&replayed1))
	require.NoError(t, connReconn.ReadJSON(&replayed2))

	assert.Equal(t, ws.EventIncidentCreated, replayed1.Type)
	assert.Equal(t, ws.EventIncidentCreated, replayed2.Type)
	assert.Equal(t, lastSeenSeq+1, replayed1.Sequence)
	assert.Equal(t, lastSeenSeq+2, replayed2.Sequence)
}

func TestIncidents_GeospatialRadiusAndSnapshotHTTP(t *testing.T) {
	server, _, _ := setupIncidentWSTestServer(t)
	defer server.Close()

	// Post incident 1: ~1km away from (28.6139, 77.2090)
	inc1Req := map[string]any{
		"description": "Nearby bio-waste overflow",
		"stream_type": "ORGANIC",
		"severity":    "MEDIUM",
		"latitude":    28.6200,
		"longitude":   77.2100,
	}
	b1, _ := json.Marshal(inc1Req)
	r1, err := http.Post(server.URL+"/api/v1/incidents", "application/json", bytes.NewReader(b1))
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, r1.StatusCode)
	_ = r1.Body.Close()

	// Post incident 2: in Mumbai (19.0760, 72.8777) ~1150km away
	inc2Req := map[string]any{
		"description": "Far away electronic scrap dump",
		"stream_type": "EWASTE",
		"severity":    "HIGH",
		"latitude":    19.0760,
		"longitude":   72.8777,
	}
	b2, _ := json.Marshal(inc2Req)
	r2, err := http.Post(server.URL+"/api/v1/incidents", "application/json", bytes.NewReader(b2))
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, r2.StatusCode)
	_ = r2.Body.Close()

	// Query nearby with radius=5km from Delhi center
	nearbyResp, err := http.Get(fmt.Sprintf("%s/api/v1/incidents/nearby?lat=28.6139&lon=77.2090&radius=5", server.URL))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, nearbyResp.StatusCode)

	var nearbyList []incidents.Incident
	require.NoError(t, json.NewDecoder(nearbyResp.Body).Decode(&nearbyList))
	_ = nearbyResp.Body.Close()

	require.Len(t, nearbyList, 1)
	assert.Equal(t, "Nearby bio-waste overflow", nearbyList[0].Description)

	// Query snapshot endpoint
	snapResp, err := http.Get(server.URL + "/api/v1/incidents/snapshot")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, snapResp.StatusCode)

	var snapData struct {
		Sequence uint64               `json:"sequence"`
		Snapshot []incidents.Incident `json:"snapshot"`
	}
	require.NoError(t, json.NewDecoder(snapResp.Body).Decode(&snapData))
	_ = snapResp.Body.Close()

	assert.Equal(t, 2, len(snapData.Snapshot))
	assert.True(t, snapData.Sequence >= 2)

	// Rebuild index endpoint
	reindexResp, err := http.Post(server.URL+"/api/v1/incidents/reindex", "application/json", nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, reindexResp.StatusCode)
	_ = reindexResp.Body.Close()
}
