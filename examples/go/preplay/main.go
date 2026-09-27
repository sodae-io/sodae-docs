package main

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"regexp"
	"slices"
	"time"

	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "sodae-examples/proto/shredstream"
)

const defaultEndpoint = "http://ams.rpc.sodae.io:10301"

var (
	fatal     = []string{"UNAUTHENTICATED", "NOT_ENTITLED", "IP_NOT_ALLOWED", "QUOTA_EXCEEDED", "AUTH_RATE_LIMITED"}
	codeInMsg = regexp.MustCompile(`\(code: ([A-Z_]+)\)`)
)

type Entry struct {
	NumHashes    uint64
	Hash         solana.Hash
	Transactions []solana.Transaction
}

func main() {
	endpoint := os.Getenv("SODAE_PREPLAY_URL")
	if endpoint == "" {
		endpoint = defaultEndpoint
	}
	token := os.Getenv("SODAE_TOKEN")
	if token == "" {
		exit("set SODAE_TOKEN to your API token")
	}
	var program *solana.PublicKey
	if len(os.Args) > 1 {
		key, err := solana.PublicKeyFromBase58(os.Args[1])
		if err != nil {
			exit("the argument must be a program id")
		}
		program = &key
	}

	delay := time.Second
	for {
		err := subscribe(endpoint, token, program, &delay)
		if err == nil {
			fmt.Fprintln(os.Stderr, "stream closed by the server")
		} else {
			code, message := describe(err)
			fmt.Fprintf(os.Stderr, "stream error %s: %s\n", orDash(code), message)
			if slices.Contains(fatal, code) {
				os.Exit(1)
			}
		}
		fmt.Fprintf(os.Stderr, "reconnecting in %s\n", delay)
		time.Sleep(delay)
		delay = min(delay*2, 30*time.Second)
	}
}

func subscribe(endpoint, token string, program *solana.PublicKey, delay *time.Duration) error {
	conn, err := dial(endpoint)
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx := metadata.AppendToOutgoingContext(context.Background(), "x-token", token)
	stream, err := pb.NewShredstreamProxyClient(conn).SubscribeEntries(ctx, &pb.SubscribeEntriesRequest{})
	if err != nil {
		return err
	}
	for {
		message, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return withTrailer(err, stream.Trailer())
		}
		*delay = time.Second
		entries, err := decodeEntries(message.Entries)
		if err != nil {
			fmt.Fprintf(os.Stderr, "slot %d: could not decode entries: %v\n", message.Slot, err)
			continue
		}
		report(message.Slot, entries, program)
	}
}

func decodeEntries(data []byte) ([]Entry, error) {
	decoder := bin.NewBinDecoder(data)
	count, err := decoder.ReadUint64(binary.LittleEndian)
	if err != nil {
		return nil, err
	}
	entries := make([]Entry, 0, min(count, 1<<16))
	for range count {
		var entry Entry
		if entry.NumHashes, err = decoder.ReadUint64(binary.LittleEndian); err != nil {
			return nil, err
		}
		hash, err := decoder.ReadNBytes(32)
		if err != nil {
			return nil, err
		}
		entry.Hash = solana.HashFromBytes(hash)
		txCount, err := decoder.ReadUint64(binary.LittleEndian)
		if err != nil {
			return nil, err
		}
		entry.Transactions = make([]solana.Transaction, 0, min(txCount, 1<<16))
		for range txCount {
			var tx solana.Transaction
			if err := tx.UnmarshalWithDecoder(decoder); err != nil {
				return nil, err
			}
			entry.Transactions = append(entry.Transactions, tx)
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func report(slot uint64, entries []Entry, program *solana.PublicKey) {
	var transactions []*solana.Transaction
	for i := range entries {
		for j := range entries[i].Transactions {
			transactions = append(transactions, &entries[i].Transactions[j])
		}
	}
	if program == nil {
		fmt.Printf("%d entries=%d transactions=%d\n", slot, len(entries), len(transactions))
		return
	}
	for _, tx := range transactions {
		keys := tx.Message.AccountKeys
		if !slices.Contains(keys, *program) {
			continue
		}
		fmt.Printf("%d %s signer=%s version=%s lookups=%d\n",
			slot, tx.Signatures[0], keys[0], version(tx), len(tx.Message.AddressTableLookups))
		for _, ix := range tx.Message.Instructions {
			fmt.Printf("  %s accounts=%d data=%dB\n", keys[ix.ProgramIDIndex], len(ix.Accounts), len(ix.Data))
		}
	}
}

func version(tx *solana.Transaction) string {
	switch tx.Message.GetVersion() {
	case solana.MessageVersionV0:
		return "0"
	case solana.MessageVersionV1:
		return "1"
	default:
		return "legacy"
	}
}

func dial(endpoint string) (*grpc.ClientConn, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	creds := insecure.NewCredentials()
	if parsed.Scheme == "https" {
		creds = credentials.NewTLS(&tls.Config{})
	}
	return grpc.NewClient(parsed.Host,
		grpc.WithTransportCredentials(creds),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64<<20)),
	)
}

type codedError struct {
	error
	code string
}

func (e codedError) Unwrap() error { return e.error }

func withTrailer(err error, trailer metadata.MD) error {
	if values := trailer.Get("x-error-code"); len(values) > 0 {
		return codedError{err, values[0]}
	}
	return err
}

func describe(err error) (code, message string) {
	var coded codedError
	if errors.As(err, &coded) {
		return coded.code, status.Convert(coded.error).Message()
	}
	message = status.Convert(err).Message()
	if match := codeInMsg.FindStringSubmatch(message); match != nil {
		code = match[1]
	}
	return code, message
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func exit(message string) {
	fmt.Fprintln(os.Stderr, message)
	os.Exit(1)
}
