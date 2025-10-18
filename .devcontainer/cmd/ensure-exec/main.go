package main

import (
	"io/fs"
	"log"
	"os"
	"path/filepath"

	"github.com/denormal/go-gitignore"
)

func main() {
	if err := os.Chdir(".."); err != nil {
		log.Fatalf("Error when changing dir to parent dir: %v", err)
		os.Exit(1) // not needed
	}
	execs, err := gitignore.NewFromFile(".execs")
	if err != nil {
		log.Fatalf("Error when reading the .execs file: %v", err)
		os.Exit(1) // not needed
	}

	filepath.Walk(execs.Base(), func(path string, info fs.FileInfo, _ error) error {
		rel, err := filepath.Rel(execs.Base(), path)
		if err != nil {
			return err
		}
		if match := execs.Relative(rel, info.IsDir()); match != nil {
			if match.Ignore() {
				if !IsExecAll(info.Mode()) {
					log.Default().Println("Making file executable: ", path)
					if err := os.Chmod(path, MakeExecByAll(info.Mode())); err != nil {
						return err
					}
				}
			}
		}
		return nil
	})

}

func IsExecByOwner(mode os.FileMode) bool {
	return mode&0100 != 0
}

func MakeExecByOwner(mode os.FileMode) os.FileMode {
	return mode | 0100
}

func IsExecAll(mode os.FileMode) bool {
	return mode&0111 == 0111
}

func MakeExecByAll(mode os.FileMode) os.FileMode {
	return mode | 0111
}
