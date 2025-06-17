package cmd

import (
	"io"
	"os"
)

func deleteFileIfExists(file string) error {
	err := os.Remove(file)

	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}

		return err
	}

	return nil
}

func copyFile(source string, dest string) error {
	sourceFile, err := os.Open(source)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	destFile, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, sourceFile)
	if err != nil {
		return err
	}
	return nil
}

func uniqueSlice[T comparable](slice []T) []T {
	seen := make(map[T]bool)
	result := []T{}

	for _, value := range slice {
		if !seen[value] {
			seen[value] = true
			result = append(result, value)
		}
	}

	return result
}

func mapKeys[T comparable](slice map[T]bool) []T {
	var keys []T
	for key := range slice {
		keys = append(keys, key)
	}

	return keys
}
