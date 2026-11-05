package archive

import (
	"context"
	"errors"
	"testing"
	"time"
)

func failedSyntheticAttachment(t *testing.T, s *Store, id string) {
	t.Helper()
	job := seedAttachment(t, s, id, []byte("synthetic failed bytes"))
	if err := s.CacheAttachment(context.Background(), job, func(context.Context, AttachmentFile) error { return errors.New("synthetic unavailable media") }); err == nil {
		t.Fatal("expected failed download")
	}
}

func TestAttachmentRecoveryClaimsOnlyRetainedFailedFilesOnce(t *testing.T) {
	c := approvedConfig()
	s, path := openTestStore(t, c)
	failedSyntheticAttachment(t, s, "failed")
	failedSyntheticAttachment(t, s, "expired")
	if _, err := s.db.Exec("UPDATE messages SET timestamp=? WHERE id='expired'", time.Now().Unix()-100*86400); err != nil {
		t.Fatal(err)
	}
	cacheSynthetic(t, s, "ready", []byte("synthetic cached bytes"))
	seedAttachment(t, s, "pending", []byte("synthetic pending bytes"))
	candidates, err := s.ClaimAttachmentRecoveries(13)
	if err != nil || len(candidates) != 1 || candidates[0].MessageID != "failed" || candidates[0].Sender != approved || candidates[0].FromMe {
		t.Fatalf("claim selection: %+v %v", candidates, err)
	}
	if again, err := s.ClaimAttachmentRecoveries(13); err != nil || len(again) != 0 {
		t.Fatalf("duplicate owner-device retry: %d %v", len(again), err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, c)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if again, err := reopened.ClaimAttachmentRecoveries(13); err != nil || len(again) != 0 {
		t.Fatalf("claim did not survive restart: %d %v", len(again), err)
	}
}

func TestRecoveredPathRequiresUnchangedIdentityAndReference(t *testing.T) {
	s, _ := openTestStore(t, approvedConfig())
	failedSyntheticAttachment(t, s, "failed")
	candidates, err := s.ClaimAttachmentRecoveries(1)
	if err != nil || len(candidates) != 1 {
		t.Fatalf("claim: %d %v", len(candidates), err)
	}
	original := candidates[0]
	for _, change := range []func(*AttachmentRecovery){
		func(c *AttachmentRecovery) { c.Ref = "stale" },
		func(c *AttachmentRecovery) { c.Sender = excluded },
		func(c *AttachmentRecovery) { c.FromMe = !c.FromMe },
		func(c *AttachmentRecovery) { c.MessageID = "another" },
		func(c *AttachmentRecovery) { c.Generation++ },
		func(c *AttachmentRecovery) { c.Descriptor.DirectPath = "/v/wrong" },
	} {
		candidate := original
		change(&candidate)
		if err := s.ReplaceRecoveredAttachment(candidate, "/v/refreshed"); err == nil {
			t.Fatal("changed claim was accepted")
		}
	}
	if err := s.ReplaceRecoveredAttachment(original, "https://example.test/file"); err == nil {
		t.Fatal("absolute recovery URL accepted")
	}
	if err := s.ReplaceRecoveredAttachment(original, "/v/refreshed?token=synthetic"); err != nil {
		t.Fatal(err)
	}
	jobs, err := s.PendingAttachments(1)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("refreshed pending: %d %v", len(jobs), err)
	}
	job := jobs[0]
	if job.Ref == original.Ref || job.Descriptor.DirectPath != "/v/refreshed?token=synthetic" {
		t.Fatal("recovery did not replace path and reference")
	}
	expected := original.Descriptor
	expected.DirectPath = job.Descriptor.DirectPath
	if !equalBytes(expected.MediaKey, job.Descriptor.MediaKey) || !equalBytes(expected.FileSHA256, job.Descriptor.FileSHA256) || !equalBytes(expected.FileEncSHA256, job.Descriptor.FileEncSHA256) || expected.FileLength != job.Descriptor.FileLength {
		t.Fatal("recovery changed integrity credentials")
	}
	if err := s.ReplaceRecoveredAttachment(original, "/v/stale-response"); err == nil {
		t.Fatal("duplicate stale response accepted")
	}
	if err := s.CacheAttachment(context.Background(), job, func(context.Context, AttachmentFile) error { return errors.New("still unavailable") }); err == nil {
		t.Fatal("expected failed download")
	}
	if again, err := s.ClaimAttachmentRecoveries(1); err != nil || len(again) != 0 {
		t.Fatalf("recovered file received another phone retry: %d %v", len(again), err)
	}
	// A path-only descriptor replay is the same file and must not reset the
	// durable owner-device attempt flag.
	if err := s.RegisterAttachment(approved, original.MessageID, original.Descriptor); err != nil {
		t.Fatal(err)
	}
	jobs, err = s.PendingAttachments(1)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("replayed pending: %d %v", len(jobs), err)
	}
	_ = s.CacheAttachment(context.Background(), jobs[0], func(context.Context, AttachmentFile) error { return errors.New("unavailable") })
	if again, err := s.ClaimAttachmentRecoveries(1); err != nil || len(again) != 0 {
		t.Fatal("path replay reset durable attempt")
	}
}

func TestAttachmentRecoveryCannotReviveRevokedOrSuspendedData(t *testing.T) {
	for _, mode := range []string{"removed", "suspended", "generation"} {
		t.Run(mode, func(t *testing.T) {
			s, path := openTestStore(t, approvedConfig())
			failedSyntheticAttachment(t, s, "failed")
			candidates, err := s.ClaimAttachmentRecoveries(1)
			if err != nil || len(candidates) != 1 {
				t.Fatalf("claim: %d %v", len(candidates), err)
			}
			switch mode {
			case "removed":
				if err := s.ApplyConfig(configWithoutChats()); err != nil {
					t.Fatal(err)
				}
			case "suspended":
				s.Suspend()
			case "generation":
				if err := s.ApplyConfig(approvedConfig()); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.ReplaceRecoveredAttachment(candidates[0], "/v/FORBIDDEN_RECOVERY_LITERAL"); err == nil {
				t.Fatal("obsolete recovery revived data")
			}
			if err := s.WithAttachmentRecovery(candidates[0], func() error { t.Fatal("obsolete recovery sent a protocol request"); return nil }); err == nil {
				t.Fatal("obsolete recovery send accepted")
			}
			assertAbsentOnDisk(t, path, "FORBIDDEN_RECOVERY_LITERAL")
		})
	}
}

func TestAttachmentRecoverySendGateAndRequeuedResponse(t *testing.T) {
	s, _ := openTestStore(t, approvedConfig())
	failedSyntheticAttachment(t, s, "failed")
	candidates, err := s.ClaimAttachmentRecoveries(1)
	if err != nil || len(candidates) != 1 {
		t.Fatalf("claim: %d %v", len(candidates), err)
	}
	candidate := candidates[0]
	sent := false
	if err := s.WithAttachmentRecovery(candidate, func() error { sent = true; return nil }); err != nil || !sent {
		t.Fatalf("current recovery write: %v", err)
	}
	if _, err := s.db.Exec("UPDATE attachments SET retry_after=0 WHERE ref=?", candidate.Ref); err != nil {
		t.Fatal(err)
	}
	if err := s.RequestAttachment(approved, candidate.MessageID); err != nil {
		t.Fatal(err)
	}
	jobs, err := s.PendingAttachments(1)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("requeued download: %d %v", len(jobs), err)
	}
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- s.CacheAttachment(context.Background(), jobs[0], func(ctx context.Context, file AttachmentFile) error {
			if _, err := file.Write([]byte("synthetic")); err != nil {
				return err
			}
			close(started)
			<-ctx.Done()
			return ctx.Err()
		})
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("old-path download did not start")
	}
	if err := s.ReplaceRecoveredAttachment(candidate, "/v/owner-refreshed?token=synthetic"); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("old-path download committed after refresh")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("old-path download was not canceled")
	}
	assertCacheEmpty(t, s)
	fresh, err := s.PendingAttachments(1)
	if err != nil || len(fresh) != 1 || fresh[0].Ref == candidate.Ref || fresh[0].Descriptor.DirectPath != "/v/owner-refreshed?token=synthetic" {
		t.Fatalf("fresh pending ref: %+v %v", fresh, err)
	}
	if err := s.CacheAttachment(context.Background(), fresh[0], func(_ context.Context, file AttachmentFile) error {
		_, err := file.Write([]byte("synthetic failed bytes"))
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func TestAttachmentRecoveryPreservesReadyBytes(t *testing.T) {
	s, _ := openTestStore(t, approvedConfig())
	failedSyntheticAttachment(t, s, "ready-later")
	candidates, err := s.ClaimAttachmentRecoveries(1)
	if err != nil || len(candidates) != 1 {
		t.Fatalf("claim: %d %v", len(candidates), err)
	}
	candidate := candidates[0]
	if _, err := s.db.Exec("UPDATE attachments SET retry_after=0 WHERE ref=?", candidate.Ref); err != nil {
		t.Fatal(err)
	}
	if err := s.RequestAttachment(approved, candidate.MessageID); err != nil {
		t.Fatal(err)
	}
	jobs, err := s.PendingAttachments(1)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("requeued download: %d %v", len(jobs), err)
	}
	if err := s.CacheAttachment(context.Background(), jobs[0], func(_ context.Context, file AttachmentFile) error {
		_, err := file.Write([]byte("synthetic failed bytes"))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.ReplaceRecoveredAttachment(candidate, "/v/unused-refresh"); err != nil {
		t.Fatal(err)
	}
	info, err := s.GetAttachmentInfo(approved, candidate.MessageID)
	if err != nil || info.Status != "ready" {
		t.Fatalf("ready cache was replaced: %+v %v", info, err)
	}
	if err := s.WithAttachmentRecovery(candidate, func() error { t.Fatal("already ready file requested another owner-device retry"); return nil }); err == nil {
		t.Fatal("ready send accepted")
	}
}
