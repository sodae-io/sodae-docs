package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
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

const (
	defaultEndpoint = "http://ams.rpc.sodae.io:10201"
	pumpAMM         = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
)

var (
	fatal     = []string{"UNAUTHENTICATED", "NOT_ENTITLED", "IP_NOT_ALLOWED", "QUOTA_EXCEEDED", "AUTH_RATE_LIMITED"}
	codeInMsg = regexp.MustCompile(`\(code: ([A-Z_]+)\)`)
)

func main() {
	endpoint := os.Getenv("SODAE_YELLOWSTONE_URL")
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

	delay := time.Second
	for {
		err := subscribe(endpoint, token, request, &delay)
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

func buildRequest(args []string) (*pb.SubscribeRequest, error) {
	mode, targets := "transactions", []string{}
	if len(args) > 0 {
		mode, targets = args[0], args[1:]
	}
	commitment := pb.CommitmentLevel_PROCESSED
	request := &pb.SubscribeRequest{Commitment: &commitment}
	switch mode {
	case "transactions":
		if len(targets) == 0 {
			targets = []string{pumpAMM}
		}
		vote, failed := false, false
		request.Transactions = map[string]*pb.SubscribeRequestFilterTransactions{
			"transactions": {Vote: &vote, Failed: &failed, AccountInclude: targets},
		}
	case "accounts":
		if len(targets) == 0 {
			return nil, errors.New("usage: yellowstone accounts <pubkey>...")
		}
		request.Accounts = map[string]*pb.SubscribeRequestFilterAccounts{
			"accounts": {Account: targets},
		}
	case "slots":
		byCommitment := false
		request.Slots = map[string]*pb.SubscribeRequestFilterSlots{
			"slots": {FilterByCommitment: &byCommitment},
		}
	default:
		return nil, fmt.Errorf("unknown mode %s; use transactions, accounts or slots", mode)
	}
	return request, nil
}

func subscribe(endpoint, token string, request *pb.SubscribeRequest, delay *time.Duration) error {
	conn, err := dial(endpoint)
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx := metadata.AppendToOutgoingContext(context.Background(), "x-token", token)
	stream, err := pb.NewGeyserClient(conn).Subscribe(ctx)
	if err != nil {
		return err
	}
	if err := stream.Send(request); err != nil {
		return err
	}
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
		case *pb.SubscribeUpdate_Ping:
			if err := stream.Send(&pb.SubscribeRequest{Ping: &pb.SubscribeRequestPing{Id: 1}}); err != nil {
				return err
			}
		case *pb.SubscribeUpdate_Transaction:
			if tx := u.Transaction.Transaction; tx != nil {
				fmt.Println(u.Transaction.Slot, base58.Encode(tx.Signature))
			}
		case *pb.SubscribeUpdate_Account:
			if account := u.Account.Account; account != nil {
				fmt.Printf("%d %s lamports=%d data=%dB owner=%s\n", u.Account.Slot,
					base58.Encode(account.Pubkey), account.Lamports, len(account.Data), base58.Encode(account.Owner))
			}
		case *pb.SubscribeUpdate_Slot:
			fmt.Println(u.Slot.Slot, u.Slot.Status)
		}
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
