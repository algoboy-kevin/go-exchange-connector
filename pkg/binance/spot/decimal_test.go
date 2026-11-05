package spot

import "testing"

func TestRoundDownUp(t *testing.T) {
	cases := []struct{ v, step, down, up string }{
		{"0.3", "0.1", "0.3", "0.3"}, // float64 would say 0.30000000000000004 / 2.9999
		{"0.0004", "0.00100000", "0.000", "0.001"},
		{"123.456789", "0.01", "123.45", "123.46"},
		{"6", "0.00001", "6.00000", "6.00000"},
		{"10", "1.00000000", "10", "10"},
		{"10.5", "1", "10", "11"},
		{"0.00018", "0.00001000", "0.00018", "0.00018"},
		{"0.000181", "0.00001000", "0.00018", "0.00019"},
		{"0", "0.1", "0.0", "0.0"},
		{"79999.99", "0.01000000", "79999.99", "79999.99"},
	}
	for _, c := range cases {
		d, err := RoundDown(c.v, c.step)
		if err != nil || d != c.down {
			t.Errorf("RoundDown(%s,%s)=%q,%v want %q", c.v, c.step, d, err, c.down)
		}
		u, err := RoundUp(c.v, c.step)
		if err != nil || u != c.up {
			t.Errorf("RoundUp(%s,%s)=%q,%v want %q", c.v, c.step, u, err, c.up)
		}
	}
	for _, bad := range [][2]string{{"x", "0.1"}, {"1", "0"}, {"1", ""}, {"-1", "0.1"}, {"1e2", "1"}} {
		if _, err := RoundDown(bad[0], bad[1]); err == nil {
			t.Errorf("RoundDown(%v) accepted", bad)
		}
	}
}

func TestDecimalHelpers(t *testing.T) {
	if !ValidDecimal("0.1") || !ValidDecimal("10") || ValidDecimal("") || ValidDecimal(".5") || ValidDecimal("1.") || ValidDecimal("-1") || ValidDecimal("1e3") || ValidDecimal(" 1") {
		t.Fatal("ValidDecimal")
	}
	if ValidPositiveDecimal("0") || ValidPositiveDecimal("0.000") || !ValidPositiveDecimal("0.001") {
		t.Fatal("ValidPositiveDecimal")
	}
	if ok, _ := IsMultipleOf("0.003", "0.001"); !ok {
		t.Fatal("IsMultipleOf")
	}
	if ok, _ := IsMultipleOf("0.0035", "0.001"); ok {
		t.Fatal("IsMultipleOf false positive")
	}
	if c, _ := CmpDecimal("10.0", "9.99"); c != 1 {
		t.Fatal("Cmp")
	}
	if c, _ := CmpDecimal("1.0", "1.00"); c != 0 {
		t.Fatal("Cmp eq")
	}
	if m, _ := MulDecimal("0.00018", "50000.10"); m != "9.000018" {
		t.Fatalf("Mul %s", m)
	}
	if m, _ := MidPrice("99.99", "100.01", 2); m != "100.00" {
		t.Fatalf("Mid %s", m)
	}
	if s, _ := ScaleDecimal("100.00", 80, 100, 2); s != "80.00" {
		t.Fatalf("Scale %s", s)
	}
	if s, _ := AddDecimal("0.1", "0.22"); s != "0.32" {
		t.Fatalf("Add %s", s)
	}
	if s, _ := AddDecimal("100.25000000", "20.5"); s != "120.75000000" {
		t.Fatalf("Add keeps the written precision: %s", s)
	}
	if AvgPrice("4.5", "1.5") != "3.00000000" || AvgPrice("1", "0") != "" || AvgPrice("x", "1") != "" {
		t.Fatal("AvgPrice")
	}
	if _, err := DivDecimal("1", "0", 2); err == nil {
		t.Fatal("div by zero")
	}
}
