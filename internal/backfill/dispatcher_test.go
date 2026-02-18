package backfill

import (
	"testing"
	"time"
)

type fakeQueue struct {
	messages []Message
}

func (q *fakeQueue) Publish(msg Message) error {
	q.messages = append(q.messages, msg)
	return nil
}

type fakeDeduper struct {
	keys map[string]time.Time
}

func newFakeDeduper() *fakeDeduper {
	return &fakeDeduper{
		keys: make(map[string]time.Time),
	}
}

func (d *fakeDeduper) Acquire(key string, ttl time.Duration, now time.Time) bool {
	expiry, ok := d.keys[key]
	if ok && now.Before(expiry) {
		return false
	}
	d.keys[key] = now.Add(ttl)
	return true
}

func TestDispatcherEnqueueMissing(t *testing.T) {
	t.Parallel()

	baseTime := time.Unix(1739836800, 0)
	config := Config{
		CoalesceWindow:             15 * time.Minute,
		DedupTTL:                   12 * time.Hour,
		MaxEnqueuesPerOrgPerMinute: 2,
	}

	testCases := []struct {
		name               string
		inputs             []MessageInput
		wantPublished      int
		wantDroppedByLimit int
	}{
		{
			name: "coalesces_same_bucket_and_dedups",
			inputs: []MessageInput{
				{
					Org:         "org-a",
					Repo:        "repo-a",
					WindowStart: baseTime.Add(1 * time.Minute),
					WindowEnd:   baseTime.Add(2 * time.Minute),
					Reason:      "scrape_error",
					Now:         baseTime,
				},
				{
					Org:         "org-a",
					Repo:        "repo-a",
					WindowStart: baseTime.Add(3 * time.Minute),
					WindowEnd:   baseTime.Add(4 * time.Minute),
					Reason:      "scrape_error",
					Now:         baseTime.Add(10 * time.Second),
				},
			},
			wantPublished:      1,
			wantDroppedByLimit: 0,
		},
		{
			name: "enforces_per_org_per_minute_limit",
			inputs: []MessageInput{
				{
					Org:         "org-a",
					Repo:        "repo-a",
					WindowStart: baseTime.Add(1 * time.Minute),
					WindowEnd:   baseTime.Add(2 * time.Minute),
					Reason:      "scrape_error",
					Now:         baseTime,
				},
				{
					Org:         "org-a",
					Repo:        "repo-b",
					WindowStart: baseTime.Add(16 * time.Minute),
					WindowEnd:   baseTime.Add(17 * time.Minute),
					Reason:      "scrape_error",
					Now:         baseTime.Add(20 * time.Second),
				},
				{
					Org:         "org-a",
					Repo:        "repo-c",
					WindowStart: baseTime.Add(31 * time.Minute),
					WindowEnd:   baseTime.Add(32 * time.Minute),
					Reason:      "scrape_error",
					Now:         baseTime.Add(40 * time.Second),
				},
			},
			wantPublished:      2,
			wantDroppedByLimit: 1,
		},
		{
			name: "limit_isolated_per_org",
			inputs: []MessageInput{
				{
					Org:         "org-a",
					Repo:        "repo-a",
					WindowStart: baseTime.Add(1 * time.Minute),
					WindowEnd:   baseTime.Add(2 * time.Minute),
					Reason:      "scrape_error",
					Now:         baseTime,
				},
				{
					Org:         "org-b",
					Repo:        "repo-a",
					WindowStart: baseTime.Add(3 * time.Minute),
					WindowEnd:   baseTime.Add(4 * time.Minute),
					Reason:      "scrape_error",
					Now:         baseTime.Add(10 * time.Second),
				},
				{
					Org:         "org-b",
					Repo:        "repo-b",
					WindowStart: baseTime.Add(5 * time.Minute),
					WindowEnd:   baseTime.Add(6 * time.Minute),
					Reason:      "scrape_error",
					Now:         baseTime.Add(20 * time.Second),
				},
			},
			wantPublished:      3,
			wantDroppedByLimit: 0,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			queue := &fakeQueue{}
			deduper := newFakeDeduper()
			dispatcher := NewDispatcher(config, queue, deduper)

			droppedByLimit := 0
			for _, input := range tc.inputs {
				result := dispatcher.EnqueueMissing(input)
				if result.DroppedByRateLimit {
					droppedByLimit++
				}
			}

			if len(queue.messages) != tc.wantPublished {
				t.Fatalf("published = %d, want %d", len(queue.messages), tc.wantPublished)
			}
			if droppedByLimit != tc.wantDroppedByLimit {
				t.Fatalf("droppedByLimit = %d, want %d", droppedByLimit, tc.wantDroppedByLimit)
			}
		})
	}
}

func TestShouldDropMessageByAge(t *testing.T) {
	t.Parallel()

	baseTime := time.Unix(1739836800, 0)
	maxAge := 24 * time.Hour

	testCases := []struct {
		name string
		msg  Message
		now  time.Time
		want bool
	}{
		{
			name: "within_age_limit",
			msg: Message{
				CreatedAt: baseTime,
			},
			now:  baseTime.Add(23 * time.Hour),
			want: false,
		},
		{
			name: "exactly_at_limit_is_not_dropped",
			msg: Message{
				CreatedAt: baseTime,
			},
			now:  baseTime.Add(24 * time.Hour),
			want: false,
		},
		{
			name: "older_than_limit_is_dropped",
			msg: Message{
				CreatedAt: baseTime,
			},
			now:  baseTime.Add(24*time.Hour + time.Second),
			want: true,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ShouldDropMessageByAge(tc.msg, tc.now, maxAge)
			if got != tc.want {
				t.Fatalf("ShouldDropMessageByAge() = %t, want %t", got, tc.want)
			}
		})
	}
}
