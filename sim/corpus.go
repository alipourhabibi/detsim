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
const corpusVersion = 1

// SeedFailure is one seed that failed, and why.
type SeedFailure struct {
	Seed uint64
	Err  error
}

// SeedEntry is one line from a corpus file
type SeedEntry struct {
	Seed   uint64
	Reason string
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
		out = append(out, SeedEntry{Seed: f.Seed, Reason: reason})
		seen[f.Seed] = true
	}

	for _, e := range old {
		if !seen[e.Seed] {
			out = append(out, e)
		}
	}
	return out
}
