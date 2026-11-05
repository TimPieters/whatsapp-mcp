package archive

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

func claimSyntheticCopy(t *testing.T, s *Store, id string) AttachmentRecovery {
	t.Helper()
	failedSyntheticAttachment(t, s, id)
	claims, err := s.ClaimAttachmentMessageCopies(8)
	if err != nil || len(claims) != 1 {
		t.Fatalf("copy claim: %d %v", len(claims), err)
	}
	return claims[0]
}

func registerSyntheticCopy(t *testing.T, s *Store, candidate AttachmentRecovery, requestID string) {
	t.Helper()
	if err := s.WithAttachmentMessageCopy(candidate, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := s.RegisterAttachmentMessageCopy(candidate, requestID); err != nil {
		t.Fatal(err)
	}
}

func TestAttachmentMessageCopyRequiresExactCorrelationAndOriginalContent(t *testing.T) {
	s, _ := openTestStore(t, approvedConfig())
	candidate := claimSyntheticCopy(t, s, "copy")
	if err := s.RegisterAttachmentMessageCopy(candidate, "request_before_send"); err == nil {
		t.Fatal("unissued request registered")
	}
	registerSyntheticCopy(t, s, candidate, "synthetic_request_1")
	if err := s.WithAttachmentMessageCopy(candidate, func() error { t.Fatal("duplicate copy request sent"); return nil }); err == nil {
		t.Fatal("duplicate copy gate accepted")
	}
	replacement := candidate.Descriptor
	replacement.DirectPath = "/v/copied-original?token=synthetic"
	replacement.MediaKey = bytes.Repeat([]byte{9}, 32)
	replacement.FileEncSHA256 = bytes.Repeat([]byte{10}, 32)
	if err := s.ReplaceAttachmentMessageCopy(candidate, "unrelated_peer_request", replacement); err == nil {
		t.Fatal("uncorrelated peer response accepted")
	}
	for _, change := range []func(*AttachmentRecovery){func(c *AttachmentRecovery) { c.MessageID = "other" }, func(c *AttachmentRecovery) { c.ChatJID = excluded }, func(c *AttachmentRecovery) { c.FromMe = !c.FromMe }, func(c *AttachmentRecovery) { c.Sender = excluded }, func(c *AttachmentRecovery) { c.Ref = "stale" }} {
		wrong := candidate
		change(&wrong)
		if err := s.ReplaceAttachmentMessageCopy(wrong, "synthetic_request_1", replacement); err == nil {
			t.Fatal("different message identity accepted")
		}
	}
	for _, change := range []func(*AttachmentDescriptor){func(d *AttachmentDescriptor) { d.FileSHA256 = bytes.Repeat([]byte{11}, 32) }, func(d *AttachmentDescriptor) { d.FileLength++ }, func(d *AttachmentDescriptor) { d.MediaType = "image" }} {
		wrong := replacement
		change(&wrong)
		if err := s.ReplaceAttachmentMessageCopy(candidate, "synthetic_request_1", wrong); err == nil {
			t.Fatal("different attachment content accepted")
		}
	}
	if err := s.ReplaceAttachmentMessageCopy(candidate, "synthetic_request_1", replacement); err != nil {
		t.Fatal(err)
	}
	jobs, err := s.PendingAttachments(1)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("copied pending job: %d %v", len(jobs), err)
	}
	job := jobs[0]
	if job.Ref == candidate.Ref || job.Descriptor.DirectPath != replacement.DirectPath || !equalBytes(job.Descriptor.MediaKey, replacement.MediaKey) || !equalBytes(job.Descriptor.FileEncSHA256, replacement.FileEncSHA256) || !equalBytes(job.Descriptor.FileSHA256, candidate.Descriptor.FileSHA256) {
		t.Fatal("copy transport credentials or original plaintext identity incorrect")
	}
	if err := s.CacheAttachment(context.Background(), job, func(_ context.Context, file AttachmentFile) error {
		_, err := file.Write([]byte("synthetic failed bytes"))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.ReplaceAttachmentMessageCopy(candidate, "synthetic_request_1", replacement); err == nil {
		t.Fatal("duplicate stale copy response accepted")
	}
}

func TestAttachmentMessageCopyLedgerIsIndependentAndDurable(t *testing.T) {
	cfg := approvedConfig()
	s, path := openTestStore(t, cfg)
	failedSyntheticAttachment(t, s, "copy")
	mediaClaims, err := s.ClaimAttachmentRecoveries(1)
	if err != nil || len(mediaClaims) != 1 {
		t.Fatalf("media claim: %d %v", len(mediaClaims), err)
	}
	copyClaims, err := s.ClaimAttachmentMessageCopies(1)
	if err != nil || len(copyClaims) != 1 {
		t.Fatalf("separate copy claim: %d %v", len(copyClaims), err)
	}
	registerSyntheticCopy(t, s, copyClaims[0], "independent_request")
	var phoneAttempted, copyAttempted int
	if err := s.db.QueryRow("SELECT phone_retry_attempted,message_copy_attempted FROM attachments WHERE ref=?", copyClaims[0].Ref).Scan(&phoneAttempted, &copyAttempted); err != nil {
		t.Fatal(err)
	}
	if phoneAttempted != 1 || copyAttempted != 1 {
		t.Fatal("copy retry changed media retry ledger")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if again, err := reopened.ClaimAttachmentMessageCopies(8); err != nil || len(again) != 0 {
		t.Fatal("copy attempt was repeated after restart")
	}
}

func TestAttachmentMessageCopyCannotReviveRevokedOrSuspendedFiles(t *testing.T) {
	for _, mode := range []string{"removed", "suspended", "generation"} {
		t.Run(mode, func(t *testing.T) {
			s, path := openTestStore(t, approvedConfig())
			candidate := claimSyntheticCopy(t, s, "copy")
			registerSyntheticCopy(t, s, candidate, "bound_request")
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
			if err := s.WithAttachmentMessageCopy(candidate, func() error { t.Fatal("stale owner-device request sent"); return nil }); err == nil {
				t.Fatal("stale send accepted")
			}
			replacement := candidate.Descriptor
			replacement.DirectPath = "/v/DENIED_COPY_RECOVERY_LITERAL"
			if err := s.ReplaceAttachmentMessageCopy(candidate, "bound_request", replacement); err == nil {
				t.Fatal("revoked copy revived attachment")
			}
			assertAbsentOnDisk(t, path, "DENIED_COPY_RECOVERY_LITERAL")
			if _, err := s.ClaimAttachmentMessageCopies(1); mode == "suspended" && !errors.Is(err, ErrSuspended) {
				t.Fatalf("suspended claim: %v", err)
			}
		})
	}
}
