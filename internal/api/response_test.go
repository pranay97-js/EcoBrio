package api_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"ecobrio/internal/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteJSON(t *testing.T) {
	rec := httptest.NewRecorder()
	payload := map[string]string{"message": "success"}

	api.WriteJSON(rec, http.StatusOK, payload)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))

	var result map[string]string
	err := json.Unmarshal(rec.Body.Bytes(), &result)
	require.NoError(t, err)
	assert.Equal(t, "success", result["message"])
}

func TestWriteError(t *testing.T) {
	rec := httptest.NewRecorder()

	api.WriteError(rec, http.StatusBadRequest, "INVALID_INPUT", "missing field")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))

	var errResp api.ErrorResponse
	err := json.Unmarshal(rec.Body.Bytes(), &errResp)
	require.NoError(t, err)
	assert.Equal(t, "INVALID_INPUT", errResp.Code)
	assert.Equal(t, "missing field", errResp.Error)
}
