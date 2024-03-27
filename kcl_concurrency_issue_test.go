package kcl_parse_file_issue

import (
	"encoding/json"
	"kcl-lang.io/kcl-go/pkg/service"
	"path"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"kcl-lang.io/kcl-go/pkg/native"
	"kcl-lang.io/kcl-go/pkg/spec/gpyrpc"
)

func Test(t *testing.T) {
	client := native.NewNativeServiceClient()
	// Compile file
	output := buildProgram(t, client)
	require.NotEmpty(t, output)

	t.Run("serial execution", func(t *testing.T) {
		for i := 0; i < 1000; i++ {
			checkExecute(t, client, output, int64(i))
		}
	})
	t.Run("parallel execution", func(t *testing.T) {
		var wg sync.WaitGroup
		threads := 10
		wg.Add(threads)
		c := make(chan int64, threads*10)
		for i := 0; i < threads; i++ {
			go func() {
				defer wg.Done()
				for id := range c {
					checkExecute(t, client, output, id)
				}
			}()
		}
		for i := 0; i < 1000; i++ {
			c <- int64(i)
		}
		close(c)
		wg.Wait()
	})
}

func buildProgram(t *testing.T, client service.KclvmService) string {
	tempDir := t.TempDir()
	result, err := client.BuildProgram(&gpyrpc.BuildProgram_Args{
		ExecArgs: &gpyrpc.ExecProgram_Args{
			KFilenameList: []string{"source.k"},
			KCodeList:     []string{`v=option("foo")`},
		},
		Output: path.Join(tempDir, "kcl"),
	})
	require.NoError(t, err, "BuildProgram returns error")
	return result.Path
}

func checkExecute(t *testing.T, client service.KclvmService, output string, id int64) {
	value := strconv.FormatInt(id, 10)
	result, err := client.ExecArtifact(&gpyrpc.ExecArtifact_Args{
		ExecArgs: &gpyrpc.ExecProgram_Args{
			Args: []*gpyrpc.CmdArgSpec{{
				Name:  "foo",
				Value: value,
			}},
		},
		Path: output,
	})
	require.NoError(t, err, "ExecArtifact returns errors")

	var config struct {
		V int64 `json:"v"`
	}
	err = json.Unmarshal([]byte(result.JsonResult), &config)
	require.NoError(t, err, "Can't parse configuration")

	require.Equal(t, id, config.V)
}
