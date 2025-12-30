package canalyzer

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	pb "github.com/milvus-io/milvus-proto/go-api/v2/tokenizerpb"
	"github.com/milvus-io/milvus/internal/util/pathutil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type mockServer struct {
	pb.UnimplementedTokenizerServer
}

func (s *mockServer) Tokenize(ctx context.Context, req *pb.TokenizationRequest) (*pb.TokenizationResponse, error) {
	ret := []*pb.Token{}
	for _, token := range strings.Split(req.Text, ",") {
		ret = append(ret, &pb.Token{
			Text: strings.TrimSpace(token),
		})
	}
	return &pb.TokenizationResponse{Tokens: ret}, nil
}

func TestAnalyzer(t *testing.T) {
	// use default analyzer.
	{
		m := "{}"
		analyzer, err := NewAnalyzer(m, "")
		assert.NoError(t, err)
		defer analyzer.Destroy()

		tokenStream := analyzer.NewTokenStream("football, basketball, pingpang")
		defer tokenStream.Destroy()

		tokens := []string{}
		for tokenStream.Advance() {
			tokens = append(tokens, tokenStream.Token())
		}
		assert.Equal(t, len(tokens), 3)
	}

	{
		m := ""
		analyzer, err := NewAnalyzer(m, "")
		assert.NoError(t, err)
		defer analyzer.Destroy()

		tokenStream := analyzer.NewTokenStream("football, basketball, pingpang")
		defer tokenStream.Destroy()

		tokens := []string{}
		for tokenStream.Advance() {
			tokens = append(tokens, tokenStream.Token())
		}
		assert.Equal(t, len(tokens), 3)
	}

	// use default tokenizer.
	{
		m := "{\"tokenizer\": \"standard\"}"
		analyzer, err := NewAnalyzer(m, "")
		assert.NoError(t, err)
		defer analyzer.Destroy()

		tokenStream := analyzer.NewTokenStream("football, basketball, pingpang")
		defer tokenStream.Destroy()

		tokens := []string{}
		for tokenStream.Advance() {
			tokens = append(tokens, tokenStream.Token())
		}
		assert.Equal(t, len(tokens), 3)
	}

	// jieba tokenizer.
	{
		m := "{\"tokenizer\": \"jieba\"}"
		analyzer, err := NewAnalyzer(m, "")
		assert.NoError(t, err)
		defer analyzer.Destroy()

		tokenStream := analyzer.NewTokenStream("张华考上了北京大学；李萍进了中等技术学校；我在百货公司当售货员：我们都有光明的前途")
		defer tokenStream.Destroy()
		for tokenStream.Advance() {
			assert.NotEmpty(t, tokenStream.Token())
		}
	}

	// grpc tokenizer.
	{
		lis, _ := net.Listen("tcp", "127.0.0.1:0")
		s := grpc.NewServer()
		pb.RegisterTokenizerServer(s, &mockServer{})
		go func() {
			if err := s.Serve(lis); err != nil {
				t.Errorf("Server exited with error: %v", err)
			}
		}()
		addr, stop := func() (string, func()) {
			lis, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatalf("failed to listen: %v", err)
			}

			s := grpc.NewServer()
			pb.RegisterTokenizerServer(s, &mockServer{})

			go func() {
				_ = s.Serve(lis)
			}()

			return lis.Addr().String(), func() {
				s.Stop()
				_ = lis.Close()
			}
		}()
		defer stop()

		m := "{\"tokenizer\": {\"type\":\"grpc\", \"endpoint\":\"http://" + addr + "\"}}"
		analyzer, err := NewAnalyzer(m, "")
		assert.NoError(t, err)
		defer analyzer.Destroy()

		tokenStream := analyzer.NewTokenStream("football, basketball, pingpang")
		defer tokenStream.Destroy()
		for tokenStream.Advance() {
			fmt.Println(tokenStream.Token())
		}
	}

	// lindera tokenizer.
	{
		m := "{\"tokenizer\": {\"type\":\"lindera\", \"dict_kind\": \"ipadic\"}}"
		tokenizer, err := NewAnalyzer(m, "")
		require.NoError(t, err)
		defer tokenizer.Destroy()

		tokenStream := tokenizer.NewTokenStream("東京スカイツリーの最寄り駅はとうきょうスカイツリー駅です")
		defer tokenStream.Destroy()
		for tokenStream.Advance() {
			fmt.Println(tokenStream.Token())
		}
	}
}

func TestValidateAnalyzer(t *testing.T) {
	InitOptions()

	// valid analyzer
	{
		m := "{\"tokenizer\": \"standard\"}"
		ids, err := ValidateAnalyzer(m, "")
		assert.NoError(t, err)
		assert.Equal(t, len(ids), 0)
	}

	{
		m := ""
		_, err := ValidateAnalyzer(m, "")
		assert.NoError(t, err)
	}

	// invalid tokenizer
	{
		m := "{\"tokenizer\": \"invalid\"}"
		_, err := ValidateAnalyzer(m, "")
		assert.Error(t, err)
	}

	// with user resource
	{
		resourcePath := pathutil.GetPath(pathutil.FileResourcePath, paramtable.GetNodeID())
		defer os.RemoveAll(resourcePath)
		UpdateParams()
		resourceID := int64(100)

		// mock remote resource file
		dir := filepath.Join(resourcePath, "default", fmt.Sprintf("%d", resourceID))
		err := os.MkdirAll(dir, os.ModePerm)
		require.NoError(t, err)

		f, err := os.Create(filepath.Join(dir, "jieba.txt"))
		require.NoError(t, err)

		f.WriteString("stop")
		f.Close()

		m := "{\"tokenizer\": \"standard\", \"filter\": [{\"type\": \"stop\", \"stop_words_file\": {\"type\": \"remote\",\"resource_name\": \"jieba_dict\", \"file_name\": \"jieba.txt\"}}]}"

		ids, err := ValidateAnalyzer(m, "{\"resource_map\": {\"jieba_dict\": 100}, \"storage_name\": \"default\"}")
		require.NoError(t, err)
		assert.Equal(t, len(ids), 1)
		assert.Equal(t, ids[0], resourceID)
	}

	// with user resource and update global resource info
	{
		resourcePath := pathutil.GetPath(pathutil.FileResourcePath, paramtable.GetNodeID())
		defer os.RemoveAll(resourcePath)
		UpdateParams()
		resourceID := int64(100)

		// mock remote resource file
		dir := filepath.Join(resourcePath, fmt.Sprintf("%d", resourceID))
		err := os.MkdirAll(dir, os.ModePerm)
		require.NoError(t, err)

		f, err := os.Create(filepath.Join(dir, "jieba.txt"))
		require.NoError(t, err)

		f.WriteString("stop")
		f.Close()

		m := "{\"tokenizer\": \"standard\", \"filter\": [{\"type\": \"stop\", \"stop_words_file\": {\"type\": \"remote\",\"resource_name\": \"jieba_dict\", \"file_name\": \"jieba.txt\"}}]}"

		// update global resource info
		err = UpdateGlobalResourceInfo(map[string]int64{"jieba_dict": resourceID})
		require.NoError(t, err)

		ids, err := ValidateAnalyzer(m, "")
		require.NoError(t, err)

		assert.Equal(t, len(ids), 1)
		assert.Equal(t, ids[0], resourceID)
	}
}
