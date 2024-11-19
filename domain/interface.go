package domain

import (
	"context"
)

type DataSource interface {
	GetData(ctx context.Context) (*Payload, error)
	Progress() Progress
	Close(ctx context.Context) error
}
