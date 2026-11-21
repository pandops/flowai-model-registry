package app

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/pandops/flowai-model-registry/internal/store"
)

var b64 = base64.RawURLEncoding

// keyAAD binds a stored task key to its claim.
func keyAAD(k store.ClaimKey) []byte {
	return []byte(fmt.Sprintf("%d|%d|%d|%d|litellm-task-key", k.TeamID, k.TaskID, k.Attempt, k.ClaimSeq))
}

// sealKey encrypts a virtual key with AES-256-GCM: key id (8 bytes), nonce,
// sealed text.
func (a *App) sealKey(k store.ClaimKey, key string) ([]byte, error) {
	id := a.deps.Config.EncryptionActiveKey
	gcm, err := newGCM(a.deps.Config.EncryptionKeys[id])
	if err != nil {
		return nil, err
	}
	out := make([]byte, 8, 8+gcm.NonceSize()+len(key)+gcm.Overhead())
	binary.BigEndian.PutUint64(out, uint64(id))
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	out = append(out, nonce...)
	return gcm.Seal(out, nonce, []byte(key), keyAAD(k)), nil
}

func (a *App) openKey(k store.ClaimKey, sealed []byte) (string, error) {
	if len(sealed) < 8 {
		return "", errors.New("stored key is malformed")
	}
	id := int64(binary.BigEndian.Uint64(sealed[:8]))
	key, ok := a.deps.Config.EncryptionKeys[id]
	if !ok {
		return "", errors.New("stored key uses an unknown encryption key")
	}
	gcm, err := newGCM(key)
	if err != nil {
		return "", err
	}
	rest := sealed[8:]
	if len(rest) < gcm.NonceSize() {
		return "", errors.New("stored key is malformed")
	}
	plain, err := gcm.Open(nil, rest[:gcm.NonceSize()], rest[gcm.NonceSize():], keyAAD(k))
	if err != nil {
		return "", errors.New("stored key does not decrypt")
	}
	return string(plain), nil
}

func newGCM(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// newVirtualKey returns a fresh LiteLLM virtual key and its token hash.
func newVirtualKey() (string, string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", "", err
	}
	key := "sk-" + b64.EncodeToString(b)
	return key, tokenOf(key), nil
}

// tokenOf is LiteLLM's hash of a virtual key.
func tokenOf(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

// claimToken is the payload task-manager signs (agents.md rule 12).
type claimToken struct {
	store.ClaimKey
	ExecutorID int64
}

func (a *App) verifyToken(tok string) (claimToken, bool) {
	parts := strings.Split(tok, ".")
	if len(parts) != 2 {
		return claimToken{}, false
	}
	payload, err1 := b64.DecodeString(parts[0])
	sig, err2 := b64.DecodeString(parts[1])
	if err1 != nil || err2 != nil || len(a.deps.Config.ClaimTokenKey) != ed25519.PublicKeySize {
		return claimToken{}, false
	}
	if !ed25519.Verify(a.deps.Config.ClaimTokenKey, payload, sig) {
		return claimToken{}, false
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(payload, &raw); err != nil {
		return claimToken{}, false
	}
	var c claimToken
	var ok [5]bool
	c.TeamID, ok[0] = jsonID(raw["team_id"])
	c.TaskID, ok[1] = jsonID(raw["task_id"])
	c.Attempt, ok[2] = jsonID(raw["attempt"])
	c.ClaimSeq, ok[3] = jsonID(raw["claim_seq"])
	c.ExecutorID, ok[4] = jsonID(raw["executor_id"])
	for _, v := range ok {
		if !v {
			return claimToken{}, false
		}
	}
	return c, true
}

// cursor is the signed position of a models-list page.
type cursor struct {
	Key      int64  `json:"k"`
	Team     int64  `json:"t"`
	Endpoint string `json:"e"`
	Archived bool   `json:"p"`
	After    string `json:"a"`
	Expires  int64  `json:"x"`
}

const cursorLifetime = 24 * time.Hour

func (a *App) signCursor(c cursor) string {
	c.Key = a.deps.Config.CursorActiveKey
	c.Expires = a.now().Add(cursorLifetime).Unix()
	payload, _ := json.Marshal(c)
	mac := hmac.New(sha256.New, a.deps.Config.CursorKeys[c.Key])
	mac.Write(payload)
	return b64.EncodeToString(payload) + "." + b64.EncodeToString(mac.Sum(nil))
}

// readCursor verifies a cursor for team, endpoint and partition. A malformed
// or tampered cursor and a cursor issued to another team are invalid_request
// (model-registry "Operator pages through models"); a cursor for another
// endpoint or partition, or an expired one, is invalid_pagination.
func (a *App) readCursor(s string, team int64, endpoint string, archived bool) (cursor, *opError) {
	parts := strings.Split(s, ".")
	if len(parts) != 2 {
		return cursor{}, errInvalid
	}
	payload, err1 := b64.DecodeString(parts[0])
	sig, err2 := b64.DecodeString(parts[1])
	if err1 != nil || err2 != nil {
		return cursor{}, errInvalid
	}
	var c cursor
	if err := json.Unmarshal(payload, &c); err != nil {
		return cursor{}, errInvalid
	}
	key, ok := a.deps.Config.CursorKeys[c.Key]
	if !ok {
		return cursor{}, errInvalid
	}
	mac := hmac.New(sha256.New, key)
	mac.Write(payload)
	if !hmac.Equal(mac.Sum(nil), sig) {
		return cursor{}, errInvalid
	}
	if c.Team != team {
		return cursor{}, errInvalid
	}
	if c.Endpoint != endpoint || c.Archived != archived || a.now().Unix() > c.Expires || c.After == "" {
		return cursor{}, errPagination
	}
	return c, nil
}
