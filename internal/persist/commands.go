package persist

import (
	"context"
	"errors"
	"time"

	"github.com/nima-ca/sylphy/internal/command"
)

// Commands adapts a Manager to command.Persistence, the interface behind SAVE,
// BGSAVE, BGREWRITEAOF and LASTSAVE.
type Commands struct{ m *Manager }

var _ command.Persistence = (*Commands)(nil)

// NewCommands returns the adapter for m.
func NewCommands(m *Manager) *Commands { return &Commands{m: m} }

// Save runs a checkpoint and returns when it is committed.
func (c *Commands) Save(ctx context.Context) error { return translate(c.m.Save(ctx)) }

// BGSave starts a checkpoint in the background.
func (c *Commands) BGSave() error { return translate(c.m.BGSave()) }

// LastSave returns when persistence last reached a checkpoint.
func (c *Commands) LastSave() time.Time { return c.m.LastSave() }

// AOFEnabled reports whether writes are logged to an append-only file.
func (c *Commands) AOFEnabled() bool { return c.m.o.Mode == ModeAOF }

func translate(err error) error {
	if errors.Is(err, ErrCheckpointRunning) {
		return command.ErrCheckpointRunning
	}
	return err
}
