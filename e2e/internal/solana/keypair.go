//go:build e2e

package solana

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"

	"github.com/mr-tron/base58"
)

// GenerateKeypair generates a new ed25519 keypair and writes it to the given
// path in Solana's JSON format (64-byte integer array: [secret_key | public_key]).
// Returns the path (renamed to include the pubkey) and the public key as a base58 string.
func GenerateKeypair(path string) (string, string, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return "", "", fmt.Errorf("failed to generate keypair: %w", err)
	}

	// Solana keypair format: 64-byte array [32-byte secret seed | 32-byte public key]
	keypairBytes := make([]byte, 64)
	copy(keypairBytes[:32], priv.Seed())
	copy(keypairBytes[32:], pub)

	intArray := make([]int, 64)
	for i, b := range keypairBytes {
		intArray[i] = int(b)
	}

	data, err := json.Marshal(intArray)
	if err != nil {
		return "", "", fmt.Errorf("failed to marshal keypair: %w", err)
	}

	if err := os.WriteFile(path, data, 0600); err != nil {
		return "", "", fmt.Errorf("failed to write keypair: %w", err)
	}

	pubkeyStr := base58.Encode(pub)

	// Rename file to include pubkey for convenience.
	newPath := path + "-" + pubkeyStr + ".json"
	if err := os.Rename(path, newPath); err != nil {
		// If rename fails, keep the original path.
		return path, pubkeyStr, nil
	}

	return newPath, pubkeyStr, nil
}

// LoadKeypair loads a Solana keypair JSON file and returns the ed25519 private key.
func LoadKeypair(path string) (ed25519.PrivateKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read keypair file: %w", err)
	}

	var intArray []int
	if err := json.Unmarshal(data, &intArray); err != nil {
		return nil, fmt.Errorf("failed to parse keypair JSON: %w", err)
	}

	if len(intArray) != 64 {
		return nil, fmt.Errorf("expected 64-byte keypair, got %d bytes", len(intArray))
	}

	seed := make([]byte, 32)
	for i := 0; i < 32; i++ {
		seed[i] = byte(intArray[i])
	}

	return ed25519.NewKeyFromSeed(seed), nil
}

// PubkeyFromKeypairJSON extracts the base58 public key from a Solana keypair JSON file.
func PubkeyFromKeypairJSON(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("failed to read keypair file: %w", err)
	}

	var intArray []int
	if err := json.Unmarshal(data, &intArray); err != nil {
		return "", fmt.Errorf("failed to parse keypair JSON: %w", err)
	}

	if len(intArray) != 64 {
		return "", fmt.Errorf("expected 64-byte keypair, got %d bytes", len(intArray))
	}

	pubBytes := make([]byte, 32)
	for i := 0; i < 32; i++ {
		pubBytes[i] = byte(intArray[32+i])
	}

	return base58.Encode(pubBytes), nil
}
