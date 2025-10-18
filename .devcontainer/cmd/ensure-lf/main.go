package main

import (
	"bytes"
	"io/fs"
	"log"
	"os"
	"path/filepath"

	"github.com/denormal/go-gitignore"
)

const targetEOL = "\n"

func main() {
	if err := os.Chdir(".."); err != nil {
		log.Fatalf("Error changing to parent dir: %v", err)
	}

	lfs, err := gitignore.NewFromFile(".execs")
	if err != nil {
		log.Fatalf("Error reading .execs file: %v", err)
	}

	err = filepath.WalkDir(lfs.Base(), func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}

		rel, err := filepath.Rel(lfs.Base(), path)
		if err != nil {
			return err
		}

		match := lfs.Relative(rel, false)
		if match == nil || !match.Ignore() {
			return nil
		}

		ok, err := HasCorrectLineEndings(path, targetEOL)
		if err != nil {
			return err
		}
		if !ok {
			log.Printf("Normalizing line endings: %s", path)
			if err := NormalizeLineEndings(path, targetEOL); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		log.Fatalf("Error applying line endings: %v", err)
	}
}

// HasCorrectLineEndings checks if a file uses the given EOL style.
func HasCorrectLineEndings(path string, eol string) (bool, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return false, err
	}

	if eol == "\n" {
		// If the file contains any CRLF, it's not correct
		return !bytes.Contains(content, []byte("\r\n")), nil
	}
	if eol == "\r\n" {
		// If it contains LF-only lines, it's not correct
		return !bytes.Contains(content, []byte("\n")) || bytes.Contains(content, []byte("\r\n")), nil
	}
	return false, nil
}

// NormalizeLineEndings replaces CRLF/CR with the desired EOL.
// Bad impl for big files, but this is only applied for small files..., streaming should be used for bigger files
func NormalizeLineEndings(path string, eol string) error {
	content, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	normalized := bytes.ReplaceAll(content, []byte("\r\n"), []byte("\n"))
	normalized = bytes.ReplaceAll(normalized, []byte("\r"), []byte("\n"))

	if eol != "\n" {
		normalized = bytes.ReplaceAll(normalized, []byte("\n"), []byte(eol))
	}

	if !bytes.Equal(content, normalized) {
		if err := os.WriteFile(path, normalized, 0o644); err != nil {
			return err
		}
	}
	return nil
}
