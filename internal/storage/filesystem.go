package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// FilesystemStore implements ObjectStore using the local filesystem.
// Keys are mapped to file paths under a base directory. Forward slashes
// in keys are converted to OS-native path separators for storage, and
// converted back to forward slashes on retrieval.
//
// FilesystemStore is stateless and safe for concurrent use.
type FilesystemStore struct {
	baseDir string
}

// NewFilesystemStore creates a FilesystemStore rooted at baseDir.
// Creates baseDir if it does not exist.
func NewFilesystemStore(baseDir string) (*FilesystemStore, error) {
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, fmt.Errorf("filesystem: create base dir: %w", err)
	}
	return &FilesystemStore{baseDir: baseDir}, nil
}

// Put writes data atomically by writing to a temp file in the target
// directory and renaming it into place. This prevents partial files on crash.
func (s *FilesystemStore) Put(_ context.Context, key string, data []byte) error {
	if key == "" {
		return fmt.Errorf("filesystem: put: key must not be empty")
	}

	path := s.keyToPath(key)
	dir := filepath.Dir(path)

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("filesystem: put: mkdir: %w", err)
	}

	// Write to a temp file in the same directory to ensure atomic rename.
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return fmt.Errorf("filesystem: put: create temp: %w", err)
	}
	tmpName := tmp.Name()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("filesystem: put: write: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("filesystem: put: close: %w", err)
	}

	if err := os.Rename(tmpName, path); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("filesystem: put: rename: %w", err)
	}
	return nil
}

// Get reads the data stored at key. Returns an error wrapping os.ErrNotExist
// if the key does not exist.
func (s *FilesystemStore) Get(_ context.Context, key string) ([]byte, error) {
	data, err := os.ReadFile(s.keyToPath(key))
	if err != nil {
		return nil, fmt.Errorf("filesystem: get: %w", err)
	}
	return data, nil
}

// List returns all keys whose path starts with the given prefix.
// Returns (nil, nil) if no keys match.
func (s *FilesystemStore) List(_ context.Context, prefix string) ([]string, error) {
	var keys []string

	err := filepath.WalkDir(s.baseDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}

		// Convert absolute path back to a key using forward slashes.
		rel, err := filepath.Rel(s.baseDir, path)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(rel)

		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("filesystem: list: %w", err)
	}
	return keys, nil
}

// Delete removes the key. Returns nil if the key does not exist (idempotent).
func (s *FilesystemStore) Delete(_ context.Context, key string) error {
	err := os.Remove(s.keyToPath(key))
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("filesystem: delete: %w", err)
	}
	return nil
}

// Exists returns true if the key exists on disk.
func (s *FilesystemStore) Exists(_ context.Context, key string) (bool, error) {
	_, err := os.Stat(s.keyToPath(key))
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, fmt.Errorf("filesystem: exists: %w", err)
}

// keyToPath converts a forward-slash key to an OS-native file path under baseDir.
func (s *FilesystemStore) keyToPath(key string) string {
	return filepath.Join(s.baseDir, filepath.FromSlash(key))
}
