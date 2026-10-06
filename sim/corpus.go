package sim

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
)

var ErrCorpusVersion = errors.New("sim: corpus version mismatch")

// corpusVersion changes when a stored seed stops meaning what it meant.
const corpusVersion = 2

// SeedFailure is one seed that failed, and why.
type SeedFailure struct {
	Seed uint64
	Err  error
	Plan *Plan
}

// SeedEntry is one seed from a corpus file, with the plan that failed.
//
// The plan is saved as text, not drawn again from the seed. A seed alone
// means a different run as soon as the plan generator changes. The plan
// keeps meaning the same faults.
type SeedEntry struct {
	Seed   uint64
	Reason string
	Plan   *Plan // nil if the file had no plan lines for this seed
}

// SaveCorpus writes failing seeds so they can be run again later.
func SaveCorpus(w io.Writer, header string, entries []SeedEntry) error {
	sorted := slices.Clone(entries)
	slices.SortFunc(sorted, func(a, b SeedEntry) int {
		switch {
		case a.Seed < b.Seed:
			return -1
		case a.Seed > b.Seed:
			return 1
		default:
			return 0
		}
	})

	bw := bufio.NewWriter(w)

	fmt.Fprintf(bw, "version %d\n", corpusVersion)
	for line := range strings.Lines(header) {
		fmt.Fprintf(bw, "# %s", line)
		if !strings.HasSuffix(line, "\n") {
			fmt.Fprintln(bw)
		}
	}
	fmt.Fprintln(bw)

	var last uint64
	first := true
	for _, e := range sorted {
		if !first && e.Seed == last {
			continue
		}
		first, last = false, e.Seed

		// One line each
		reason := strings.TrimSpace(strings.SplitN(e.Reason, "\n", 2)[0])
		fmt.Fprintf(bw, "%d\t%s\n", e.Seed, reason)

		// The plan goes under its seed, one tab in.
		if e.Plan != nil {
			for _, f := range e.Plan.Faults {
				fmt.Fprintf(bw, "\tt=%d %s\n", f.At, f.Fault)
			}
			for _, name := range e.Plan.Buggify {
				fmt.Fprintf(bw, "\tbuggify %s\n", name)
			}
		}
	}

	return bw.Flush()
}

// LoadCorpus reads back the seeds.
// Fails on a version mismatch.
func LoadCorpus(r io.Reader) ([]SeedEntry, error) {
	sc := bufio.NewScanner(r)

	if !sc.Scan() {
		return nil, fmt.Errorf("sim: empty corpus")
	}
	var got int
	if _, err := fmt.Sscanf(sc.Text(), "version %d", &got); err != nil {
		return nil, fmt.Errorf("sim: corpus has no version line")
	}
	if got != corpusVersion {
		return nil, fmt.Errorf("%w: file is %d, this build reads %d",
			ErrCorpusVersion, got, corpusVersion)
	}

	var entries []SeedEntry
	for line := 2; sc.Scan(); line++ {
		text := strings.TrimSpace(sc.Text())
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}

		if strings.HasPrefix(sc.Text(), "\t") {
			if len(entries) == 0 {
				return nil, fmt.Errorf("sim: corpus line %d: plan line with no seed above it", line)
			}
			e := &entries[len(entries)-1]
			if e.Plan == nil {
				e.Plan = &Plan{}
			}
			if err := e.Plan.addLine(text); err != nil {
				return nil, fmt.Errorf("sim: corpus line %d: %w", line, err)
			}
			continue
		}

		field, reason, _ := strings.Cut(text, "\t")
		seed, err := strconv.ParseUint(strings.TrimSpace(field), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("sim: corpus line %d: %q is not a seed", line, field)
		}
		entries = append(entries, SeedEntry{
			Seed:   seed,
			Reason: strings.TrimSpace(reason),
		})
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}

	return entries, nil
}

// Seeds pulls out just the numbers, for a caller that only wants to run them.
func Seeds(entries []SeedEntry) []uint64 {
	out := make([]uint64, len(entries))
	for i, e := range entries {
		out[i] = e.Seed
	}
	return out
}

// MergeCorpus adds new failures to seeds already on file.
func MergeCorpus(old []SeedEntry, fails []SeedFailure) []SeedEntry {
	out := make([]SeedEntry, 0, len(old)+len(fails))
	seen := make(map[uint64]bool, len(fails))

	for _, f := range fails {
		reason := ""
		if f.Err != nil {
			reason = f.Err.Error()
		}
		out = append(out, SeedEntry{Seed: f.Seed, Reason: reason, Plan: f.Plan})
		seen[f.Seed] = true
	}

	for _, e := range old {
		if !seen[e.Seed] {
			out = append(out, e)
		}
	}
	return out
}

// addLine reads one saved plan line: "t=120 <fault>" or "buggify <name>".
func (p *Plan) addLine(text string) error {
	if name, ok := strings.CutPrefix(text, "buggify "); ok {
		p.Buggify = append(p.Buggify, name)
		return nil
	}
	at, rest, ok := strings.Cut(strings.TrimPrefix(text, "t="), " ")
	if !ok || !strings.HasPrefix(text, "t=") {
		return fmt.Errorf("%q is not a plan line", text)
	}
	t, err := strconv.ParseInt(at, 10, 64)
	if err != nil {
		return fmt.Errorf("%q has a bad time", text)
	}
	f, err := ParseFault(rest)
	if err != nil {
		return err
	}
	p.Faults = append(p.Faults, Scheduled{At: Time(t), Fault: f})
	return nil
}
