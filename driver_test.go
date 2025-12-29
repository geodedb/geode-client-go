package geodesql

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDriver_TypedErrors(t *testing.T) {
	c, err := newConn(&dsnConfig{})
	if err != nil {
		t.Skip("requires geoded in PATH or adjust DSN in test to process mode")
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// US001
	_, err = c.QueryContext(ctx, "RETURN 1 LIMIT 10 OFFSET 5", nil)
	var derr *DriverError
	if !errors.As(err, &derr) { t.Fatalf("expected DriverError, got %v", err) }
	if derr.Code != "US001" || derr.Anchor == "" { t.Fatalf("bad US001 fields: %+v", derr) }
	// IllegalOrderKey
	_, err = c.QueryContext(ctx, "RETURN a ORDER BY f(a)", nil)
	if !errors.As(err, &derr) { t.Fatalf("expected DriverError, got %v", err) }
	if derr.Code != "IllegalOrderKey" || derr.Anchor == "" { t.Fatalf("bad IllegalOrderKey fields: %+v", derr) }
}
