package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/url"
	"os"
	"regexp"
	"slices"
	"time"

	"github.com/mr-tron/base58"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "sodae-examples/proto/geyser"
)

const defaultEndpoint = "http://ams.rpc.sodae.io:10301"

var (
	fatal     = []string{"UNAUTHENTICATED", "NOT_ENTITLED", "IP_NOT_ALLOWED", "QUOTA_EXCEEDED", "AUTH_RATE_LIMITED"}
	codeInMsg = regexp.MustCompile(`\(code: ([A-Z_]+)\)`)
)

func main() {
	endpoint := os.Getenv("SODAE_PREPLAY_URL")
	if endpoint == "" {
		endpoint = defaultEndpoint
	}
	token := os.Getenv("SODAE_TOKEN")
	if token == "" {
		exit("set SODAE_TOKEN to your API token")
	}
	request, err := buildRequest(os.Args[1:])
	if err != nil {
		exit(err.Error())
	}
	detailed := len(os.Args) > 1

	delay := time.Second
	for {
		err := subscribe(endpoint, token, request, detailed, &delay)
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

func buildRequest(args []string) (*pb.SubscribeDeshredRequest, error) {
	vote := false
	filter := &pb.SubscribeRequestFilterDeshredTransactions{Vote: &vote}
	if len(args) > 0 {
		if key, err := base58.Decode(args[0]); err != nil || len(key) != 32 {
			return nil, errors.New("the argument must be a program id")
		}
		filter.AccountInclude = []string{args[0]}
	}
	return &pb.SubscribeDeshredRequest{
		DeshredTransactions: map[string]*pb.SubscribeRequestFilterDeshredTransactions{"preplay": filter},
	}, nil
}

func subscribe(endpoint, token string, request *pb.SubscribeDeshredRequest, detailed bool, delay *time.Duration) error {
	conn, err := dial(endpoint)
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx := metadata.AppendToOutgoingContext(context.Background(), "x-token", token)
	stream, err := pb.NewGeyserClient(conn).SubscribeDeshred(ctx)
	if err != nil {
		return err
	}
	if err := stream.Send(request); err != nil {
		return err
	}
	perSlot := map[uint64]int{}
	for {
		update, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return withTrailer(err, stream.Trailer())
		}
		*delay = time.Second
		switch u := update.UpdateOneof.(type) {
		case *pb.SubscribeUpdateDeshred_Ping:
			if err := stream.Send(&pb.SubscribeDeshredRequest{Ping: &pb.SubscribeRequestPing{Id: 1}}); err != nil {
				return err
			}
		case *pb.SubscribeUpdateDeshred_DeshredTransaction:
			tx, slot := u.DeshredTransaction.GetTransaction(), u.DeshredTransaction.GetSlot()
			if tx == nil {
				continue
			}
			if detailed {
				printTransaction(slot, tx)
				continue
			}
			perSlot[slot]++
			for len(perSlot) > 2 {
				oldest := slices.Min(slices.Collect(maps.Keys(perSlot)))
				fmt.Printf("%d transactions=%d\n", oldest, perSlot[oldest])
				delete(perSlot, oldest)
			}
		}
	}
}

func printTransaction(slot uint64, tx *pb.SubscribeUpdateDeshredTransactionInfo) {
	message := tx.GetTransaction().GetMessage()
	if message == nil {
		return
	}
	key := func(i uint32) string {
		if int(i) < len(message.AccountKeys) {
			return base58.Encode(message.AccountKeys[i])
		}
		return ""
	}
	version := "legacy"
	if message.Versioned {
		version = "0"
		if message.Config != nil {
			version = "1"
		}
	}
	fmt.Printf("%d %s signer=%s version=%s lookups=%d loaded=%d\n", slot, base58.Encode(tx.Signature), key(0), version,
		len(message.AddressTableLookups), len(tx.LoadedWritableAddresses)+len(tx.LoadedReadonlyAddresses))
	for _, ix := range message.Instructions {
		fmt.Printf("  %s accounts=%d data=%dB\n", key(ix.ProgramIdIndex), len(ix.Accounts), len(ix.Data))
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
