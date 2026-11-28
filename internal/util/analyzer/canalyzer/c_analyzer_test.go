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

	pb "github.com/milvus-io/milvus-proto/go-api/v3/tokenizerpb"
	"github.com/milvus-io/milvus/internal/util/pathutil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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

		tokenStream, err := analyzer.NewTokenStream("football, basketball, pingpang")
		assert.NoError(t, err)
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

		tokenStream, err := analyzer.NewTokenStream("football, basketball, pingpang")
		assert.NoError(t, err)
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

		tokenStream, err := analyzer.NewTokenStream("football, basketball, pingpang")
		assert.NoError(t, err)
		defer tokenStream.Destroy()

		tokens := []string{}
		for tokenStream.Advance() {
			tokens = append(tokens, tokenStream.Token())
		}
		assert.Equal(t, len(tokens), 3)
	}

	// use char_filter before tokenizer.
	{
		m := `{"char_filter": [{"type": "mapping", "mappings": ["& => and"]}], "tokenizer": "standard", "filter": ["lowercase"]}`
		analyzer, err := NewAnalyzer(m, "")
		require.NoError(t, err)
		defer analyzer.Destroy()

		tokenStream, err := analyzer.NewTokenStream("A&B")
		require.NoError(t, err)
		defer tokenStream.Destroy()

		require.True(t, tokenStream.Advance())
		token := tokenStream.DetailedToken()
		assert.Equal(t, "aandb", token.GetToken())
		assert.Equal(t, int64(0), token.GetStartOffset())
		assert.Equal(t, int64(3), token.GetEndOffset())
		assert.False(t, tokenStream.Advance())
	}

	// preserve source spans when a mapping expands into multiple tokens.
	{
		m := `{"char_filter": [{"type": "mapping", "mappings": ["ab=>x y"]}], "tokenizer": "standard"}`
		analyzer, err := NewAnalyzer(m, "")
		require.NoError(t, err)
		defer analyzer.Destroy()

		tokenStream, err := analyzer.NewTokenStream("ab")
		require.NoError(t, err)
		defer tokenStream.Destroy()

		require.True(t, tokenStream.Advance())
		token := tokenStream.DetailedToken()
		assert.Equal(t, "x", token.GetToken())
		assert.Equal(t, int64(0), token.GetStartOffset())
		assert.Equal(t, int64(2), token.GetEndOffset())

		require.True(t, tokenStream.Advance())
		token = tokenStream.DetailedToken()
		assert.Equal(t, "y", token.GetToken())
		assert.Equal(t, int64(0), token.GetStartOffset())
		assert.Equal(t, int64(2), token.GetEndOffset())
		assert.False(t, tokenStream.Advance())
	}

	// preserve character boundaries when boundary offset mode is requested.
	{
		m := `{"char_filter": [{"type": "mapping", "mappings": ["ab=>x y"]}], "char_filter_offset_mode": "boundary", "tokenizer": "standard"}`
		analyzer, err := NewAnalyzer(m, "")
		require.NoError(t, err)
		defer analyzer.Destroy()

		tokenStream, err := analyzer.NewTokenStream("ab")
		require.NoError(t, err)
		defer tokenStream.Destroy()

		require.True(t, tokenStream.Advance())
		token := tokenStream.DetailedToken()
		assert.Equal(t, "x", token.GetToken())
		assert.Equal(t, int64(0), token.GetStartOffset())
		assert.Equal(t, int64(1), token.GetEndOffset())

		require.True(t, tokenStream.Advance())
		token = tokenStream.DetailedToken()
		assert.Equal(t, "y", token.GetToken())
		assert.Equal(t, int64(1), token.GetStartOffset())
		assert.Equal(t, int64(2), token.GetEndOffset())
		assert.False(t, tokenStream.Advance())
	}

	// jieba tokenizer.
	{
		m := "{\"tokenizer\": \"jieba\"}"
		analyzer, err := NewAnalyzer(m, "")
		assert.NoError(t, err)
		defer analyzer.Destroy()

		tokenStream, err := analyzer.NewTokenStream("张华考上了北京大学；李萍进了中等技术学校；我在百货公司当售货员：我们都有光明的前途")
		assert.NoError(t, err)
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

		tokenStream, err := analyzer.NewTokenStream("football, basketball, pingpang")
		assert.NoError(t, err)
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

		tokenStream, err := tokenizer.NewTokenStream("東京スカイツリーの最寄り駅はとうきょうスカイツリー駅です")
		assert.NoError(t, err)
		defer tokenStream.Destroy()
		for tokenStream.Advance() {
			fmt.Println(tokenStream.Token())
		}
	}
}

func TestValidateAnalyzer(t *testing.T) {
	require.NoError(t, InitOptions())

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

	// valid char_filter
	{
		m := `{"char_filter": [{"type": "mapping", "mappings": ["& => and"]}], "tokenizer": "standard"}`
		ids, err := ValidateAnalyzer(m, "")
		assert.NoError(t, err)
		assert.Equal(t, len(ids), 0)
	}

	// unsupported char_filter type
	{
		m := `{"char_filter": [{"type": "unsupported"}], "tokenizer": "standard"}`
		_, err := ValidateAnalyzer(m, "")
		assert.Error(t, err)
	}

	// char_filter is not supported with build-in analyzer type in the first version.
	{
		m := `{"type": "standard", "char_filter": []}`
		_, err := ValidateAnalyzer(m, "")
		assert.Error(t, err)
	}

	// with user resource
	{
		resourcePath := pathutil.GetPath(pathutil.FileResourcePath, paramtable.GetNodeID())
		defer os.RemoveAll(resourcePath)
		require.NoError(t, updateParams())
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
		require.NoError(t, updateParams())
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

func TestBuildRuntimeOptions(t *testing.T) {
	params := paramtable.Get()
	localResourcePath := "/tmp/milvus-analyzer-test"
	urlKey := "function.analyzer.lindera.download_urls.ipadic"
	require.NoError(t, params.Save(params.FunctionCfg.LocalResourcePath.Key, localResourcePath))
	require.NoError(t, params.SaveGroup(map[string]string{urlKey: "http://a.test/ipadic.tar.gz, http://b.test/ipadic.tar.gz"}))
	defer params.Reset(params.FunctionCfg.LocalResourcePath.Key)
	defer params.Reset(urlKey)

	options := BuildRuntimeOptions()
	assert.Equal(t, localResourcePath, options[DefaultDictPathKey])
	assert.Equal(t, pathutil.GetPath(pathutil.FileResourcePath, paramtable.GetNodeID()), options[ResourcePathKey])
	assert.Equal(t,
		map[string][]string{"ipadic": {"http://a.test/ipadic.tar.gz", "http://b.test/ipadic.tar.gz"}},
		options[LinderaDictURLKey],
	)
	assert.Equal(t,
		map[string][]string{"ipadic": {"http://a.test/ipadic.tar.gz"}},
		buildLinderaDownloadURLs(map[string]string{".ipadic": "http://a.test/ipadic.tar.gz"}),
	)
	assert.Equal(t, []string{localResourcePath, "/var/lib/milvus/dicts"}, options[AllowedLocalDirsKey])

	dirsKey := params.FunctionCfg.AllowedLocalResourceDirs.Key
	require.NoError(t, params.Save(dirsKey, " /data/dicts/ , relative/dicts, ,/opt/stop"))
	defer params.Reset(dirsKey)
	assert.Equal(t, []string{localResourcePath, "/data/dicts", "/opt/stop"}, buildAllowedLocalDirs())
}

func TestAnalyzerLocalResourceDirs(t *testing.T) {
	params := paramtable.Get()
	allowed := t.TempDir()
	dirsKey := params.FunctionCfg.AllowedLocalResourceDirs.Key
	require.NoError(t, params.Save(dirsKey, allowed))
	defer func() {
		params.Reset(dirsKey)
		require.NoError(t, updateParams())
	}()
	require.NoError(t, updateParams())

	stopWords := filepath.Join(allowed, "stop.txt")
	require.NoError(t, os.WriteFile(stopWords, []byte("the\nand\n"), 0o600))
	outside := filepath.Join(t.TempDir(), "secret.txt")
	require.NoError(t, os.WriteFile(outside, []byte("root:x:0:0:root:/root:/bin/bash secret-value\n"), 0o600))

	stopParams := func(path string) string {
		return fmt.Sprintf(`{"tokenizer": "standard", "filter": [{"type": "stop", "stop_words_file": {"type": "local", "path": %q}}]}`, path)
	}

	// A file under an allowed directory still works.
	analyzer, err := NewAnalyzer(stopParams(stopWords), "")
	require.NoError(t, err)
	defer analyzer.Destroy()
	tokenStream, err := analyzer.NewTokenStream("the cat and the dog")
	require.NoError(t, err)
	defer tokenStream.Destroy()
	tokens := []string{}
	for tokenStream.Advance() {
		tokens = append(tokens, tokenStream.Token())
	}
	assert.Equal(t, []string{"cat", "dog"}, tokens)

	// Paths outside the allowed directories are rejected for every file-backed option.
	for _, path := range []string{outside, "/etc/passwd", "/dev/zero", "/proc/self/environ"} {
		for _, params := range []string{
			stopParams(path),
			fmt.Sprintf(`{"tokenizer": {"type": "jieba", "extra_dict_file": {"type": "local", "path": %q}}}`, path),
			fmt.Sprintf(`{"tokenizer": "standard", "filter": [{"type": "synonym", "synonyms_file": {"type": "local", "path": %q}}]}`, path),
			fmt.Sprintf(`{"tokenizer": "standard", "filter": [{"type": "decompounder", "word_list_file": {"type": "local", "path": %q}}]}`, path),
		} {
			_, err := NewAnalyzer(params, "")
			require.Error(t, err, params)
			assert.Contains(t, err.Error(), "allowed_local_resource_dirs", params)
			assert.NotContains(t, err.Error(), "secret-value", params)
			assert.NotContains(t, err.Error(), "root:x", params)

			_, err = ValidateAnalyzer(params, "")
			require.Error(t, err, params)
			assert.NotContains(t, err.Error(), "root:x", params)
		}
	}

	// Parse errors of an allowed file report the line, not its content.
	// Line 1 is valid for both formats; line 2 is not.
	badDict := filepath.Join(allowed, "bad_dict.txt")
	require.NoError(t, os.WriteFile(badDict, []byte("word\nroot:x:0:0:root:/root:/bin/bash secret-value\n"), 0o600))
	for _, params := range []string{
		fmt.Sprintf(`{"tokenizer": {"type": "jieba", "extra_dict_file": {"type": "local", "path": %q}}}`, badDict),
		fmt.Sprintf(`{"tokenizer": "standard", "filter": [{"type": "synonym", "synonyms_file": {"type": "local", "path": %q}}]}`, badDict),
	} {
		_, err := NewAnalyzer(params, "")
		require.Error(t, err, params)
		assert.Contains(t, err.Error(), "line 2", params)
		assert.NotContains(t, err.Error(), "secret-value", params)
		assert.NotContains(t, err.Error(), "root:x", params)
	}
}

func TestRemoteResourceFileNameStaysInResourceDir(t *testing.T) {
	resourcePath := pathutil.GetPath(pathutil.FileResourcePath, paramtable.GetNodeID())
	defer os.RemoveAll(resourcePath)
	require.NoError(t, updateParams())
	resourceID := int64(101)
	dir := filepath.Join(resourcePath, fmt.Sprintf("%d", resourceID))
	require.NoError(t, os.MkdirAll(dir, os.ModePerm))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "stop.txt"), []byte("stop"), 0o600))
	require.NoError(t, UpdateGlobalResourceInfo(map[string]int64{"stop_dict": resourceID}))

	remote := func(fileName string) string {
		return fmt.Sprintf(`{"tokenizer": "standard", "filter": [{"type": "stop", "stop_words_file": {"type": "remote", "resource_name": "stop_dict", "file_name": %q}}]}`, fileName)
	}
	ids, err := ValidateAnalyzer(remote("stop.txt"), "")
	require.NoError(t, err)
	assert.Equal(t, []int64{resourceID}, ids)

	for _, fileName := range []string{"../../../../../../../../etc/passwd", "../101/stop.txt", "sub/stop.txt", "/etc/passwd"} {
		_, err := NewAnalyzer(remote(fileName), "")
		require.Error(t, err, fileName)
		assert.Contains(t, err.Error(), "plain file name", fileName)
	}
}
