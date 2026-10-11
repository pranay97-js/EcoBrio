package ai

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"ecobrio/internal/api"
	"ecobrio/internal/queue"
	"github.com/google/uuid"
)

const CitizenIntakeTopic = "citizen-intake"

type intakeQueueMessage struct {
	IntakeID uuid.UUID `json:"intake_id"`
}

// IntakeService coordinates asynchronous citizen intake and background worker processing
type IntakeService struct {
	store    IntakeStore
	tools    *ToolRegistry
	producer queue.Producer
}

// NewIntakeService creates an asynchronous intake coordinator
func NewIntakeService(store IntakeStore, tools *ToolRegistry, producer queue.Producer) *IntakeService {
	return &IntakeService{
		store:    store,
		tools:    tools,
		producer: producer,
	}
}

// SubmitAsyncIntake persists citizen intake as RECEIVED and dispatches to queue
func (s *IntakeService) SubmitAsyncIntake(ctx context.Context, session SessionContext, text string, coords *[2]float64) (*CitizenIntake, error) {
	trimmed := text
	if trimmed == "" {
		return nil, fmt.Errorf("%w: empty citizen narrative", api.ErrInvalidInput)
	}

	stream, _ := classifyStream(stringsToLower(trimmed))
	if stream == "" {
		return nil, fmt.Errorf("%w: ambiguous waste category; please specify material stream", api.ErrInvalidInput)
	}

	severity := classifySeverity(stringsToLower(trimmed))

	lat, lon := 28.6139, 77.2090
	if coords != nil {
		lat, lon = coords[0], coords[1]
	} else {
		lat, lon = extractCoordinates(trimmed, session)
	}

	intake := &CitizenIntake{
		ID:                uuid.New(),
		CitizenID:         session.CitizenID,
		RawText:           trimmed,
		ExtractedStream:   stream,
		ExtractedSeverity: severity,
		Latitude:          lat,
		Longitude:         lon,
		Status:            IntakeStatusReceived,
		CreatedAt:         time.Now(),
		UpdatedAt:         time.Now(),
	}

	if err := s.store.Create(ctx, intake); err != nil {
		return nil, fmt.Errorf("failed saving citizen intake: %w", err)
	}

	// Dispatch to background intake queue
	if s.producer != nil {
		payload, _ := json.Marshal(intakeQueueMessage{IntakeID: intake.ID})
		if err := s.producer.Publish(ctx, CitizenIntakeTopic, payload); err != nil {
			slog.Warn("Failed to enqueue citizen intake message", "intake_id", intake.ID, "error", err)
		}
	}

	return intake, nil
}

// ProcessIntakeItem handles the asynchronous progression: RECEIVED -> PROCESSING -> INCIDENT_CREATED
func (s *IntakeService) ProcessIntakeItem(ctx context.Context, intakeID uuid.UUID) error {
	intake, err := s.store.GetByID(ctx, intakeID)
	if err != nil {
		return err
	}

	// Transition to PROCESSING
	intake.Status = IntakeStatusProcessing
	_ = s.store.Update(ctx, intake)

	// Invoke deterministic incident tool
	inc, err := s.tools.ExecuteReportWasteIncident(ctx, SessionContext{CitizenID: intake.CitizenID}, ReportIncidentParams{
		Description:        intake.RawText,
		CategorySuggestion: intake.ExtractedStream,
		Severity:           intake.ExtractedSeverity,
		Latitude:           intake.Latitude,
		Longitude:          intake.Longitude,
	})

	if err != nil {
		intake.Status = IntakeStatusFailed
		intake.ErrorMessage = err.Error()
		_ = s.store.Update(ctx, intake)
		return fmt.Errorf("failed executing report tool in worker: %w", err)
	}

	// Transition to INCIDENT_CREATED
	intake.Status = IntakeStatusIncidentCreated
	intake.IncidentID = &inc.ID
	intake.ErrorMessage = ""
	if err := s.store.Update(ctx, intake); err != nil {
		return fmt.Errorf("failed updating intake to completed: %w", err)
	}

	slog.Info("Citizen intake successfully processed into verified incident",
		"intake_id", intake.ID,
		"incident_id", inc.ID,
		"stream", inc.StreamType,
	)
	return nil
}

// StartBackgroundWorker listens on the intake queue and processes citizen reports
func (s *IntakeService) StartBackgroundWorker(ctx context.Context, consumer queue.Consumer) {
	if consumer == nil {
		return
	}

	go func() {
		_ = consumer.Consume(ctx, CitizenIntakeTopic, func(cCtx context.Context, msg []byte) error {
			var qm intakeQueueMessage
			if err := json.Unmarshal(msg, &qm); err != nil {
				slog.Error("Malformed intake queue payload", "error", err)
				return nil
			}
			return s.ProcessIntakeItem(cCtx, qm.IntakeID)
		})
	}()
}

// GetIntake returns current status and details of an intake request
func (s *IntakeService) GetIntake(ctx context.Context, id uuid.UUID) (*CitizenIntake, error) {
	return s.store.GetByID(ctx, id)
}

func stringsToLower(s string) string {
	var b []byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		b = append(b, c)
	}
	return string(b)
}
