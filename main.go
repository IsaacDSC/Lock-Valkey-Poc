package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// ErrLockNotAvailable indica que a chave já está bloqueada por outro cliente.
var ErrLockNotAvailable = errors.New("lock not available")

// Libera o lock só se o valor ainda for o token deste cliente (evita apagar lock de outro).
const unlockScript = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0
`

// ValkeyLock implementa um lock distribuído (protocolo Redis/RESP) com SET key token NX EX ttl.
type ValkeyLock struct {
	rdb *redis.Client
}

func randomToken() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

// Acquire tenta obter o lock uma vez. Sucesso: retorna token para Release.
// ttl > 0 define expiração (EX/PX); ttl==0 deixaria a chave sem TTL (não recomendado para lock).
func (l *ValkeyLock) Acquire(ctx context.Context, key string, ttl time.Duration) (token string, err error) {
	token = randomToken()
	_, err = l.rdb.SetArgs(ctx, key, token, redis.SetArgs{
		Mode: "NX",
		TTL:  ttl, // KeepTTL não se aplica aqui (não há chave pré-existente a preservar)
	}).Result()
	if errors.Is(err, redis.Nil) {
		return "", ErrLockNotAvailable
	}
	if err != nil {
		return "", err
	}
	return token, nil
}

// Release remove o lock apenas se o token coincidir.
func (l *ValkeyLock) Release(ctx context.Context, key, token string) error {
	n, err := l.rdb.Eval(ctx, unlockScript, []string{key}, token).Int64()
	if err != nil {
		return err
	}
	if n == 0 {
		return errors.New("lock not held or token mismatch")
	}
	return nil
}

// AcquireWait tenta obter o lock com backoff até ctx cancelar ou obter sucesso.
func (l *ValkeyLock) AcquireWait(ctx context.Context, key string, ttl, retryInterval time.Duration) (token string, err error) {
	for {
		token, err = l.Acquire(ctx, key, ttl)
		if err == nil {
			return token, nil
		}
		if !errors.Is(err, ErrLockNotAvailable) {
			return "", err
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(retryInterval):
		}
	}
}

func main() {
	addr := os.Getenv("VALKEY_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6378"
	}

	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()

	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("valkey inacessível (%s): %v", addr, err)
	}

	lockKey := "demo:critical-section"
	rl := &ValkeyLock{rdb: rdb}

	var wg sync.WaitGroup
	for i := range 5 {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			tryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()

			token, err := rl.AcquireWait(tryCtx, lockKey, 5*time.Second, 20*time.Millisecond)
			if err != nil {
				log.Printf("worker %d: %v", workerID, err)
				return
			}
			defer func() {
				if err := rl.Release(context.Background(), lockKey, token); err != nil {
					log.Printf("worker %d release: %v", workerID, err)
				}
			}()

			fmt.Printf("worker %d: região crítica (lock adquirido)\n", workerID)
			time.Sleep(150 * time.Millisecond)
		}(i)
	}

	wg.Wait()
	fmt.Println("concluído")
}
