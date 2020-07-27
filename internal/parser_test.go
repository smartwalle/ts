package internal

import (
	"fmt"
	"testing"
	"time"
)

func TestParser_Parse(t *testing.T) {
	var p = NewParser(Minute | Hour | Dom | Month | Dow | Descriptor)
	var deadLine = time.Now().AddDate(0, 0, 5)
	var spec = fmt.Sprintf("%d %d %d %d *", deadLine.Minute(), deadLine.Hour(), deadLine.Day(), int(deadLine.Month()))
	var s, _ = p.Parse(spec)
	t.Log(s.Next(time.Now()))
}
