package kcl_parse_file_issue

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"kcl-lang.io/kcl-go/pkg/3rdparty/dlopen"
	"kcl-lang.io/kcl-go/pkg/native"
	_ "kcl-lang.io/kcl-go/pkg/plugin/hello_plugin"
	"kcl-lang.io/kcl-go/pkg/service"
	"kcl-lang.io/kcl-go/pkg/spec/gpyrpc"
	"os"
	"path"
	"strconv"
	"sync"
	"testing"
)

const sourceSimple = `
v=option("foo")
`
const sourceRegex = `
import regex

v=option("foo")
x=regex.match("foo", "^\\w+$")
`
const sourceWithPlugins = `
import kcl_plugin.hello

v=hello.add(option("foo"), 0)
`

func TestExecArtifact(t *testing.T) {
	client := native.NewNativeServiceClient()

	threads := 10
	t.Run("serial execution", func(t *testing.T) {
		// Compile file
		binary := buildProgram(t, client, sourceSimple)
		require.NotEmpty(t, binary)
		// Run
		for i := 0; i < 1000; i++ {
			checkExecute(t, client, binary, int64(i))
		}
	})
	t.Run("serial execution (plugins)", func(t *testing.T) {
		// Compile file
		binary := buildProgram(t, client, sourceWithPlugins)
		require.NotEmpty(t, binary)
		// Run
		for i := 0; i < 1000; i++ {
			checkExecute(t, client, binary, int64(i))
		}
	})
	t.Run("serial execution (dlopen)", func(t *testing.T) {
		// Compile file
		binary := buildProgram(t, client, sourceSimple)
		require.NotEmpty(t, binary)

		// Load
		handle, err := dlopen.GetHandle([]string{binary})
		require.NoError(t, err)
		defer handle.Close()

		// Run
		for i := 0; i < 1000; i++ {
			checkExecute(t, client, binary, int64(i))
		}
	})
	t.Run("serial execution (dlopen, plugins)", func(t *testing.T) {
		// Compile file
		binary := buildProgram(t, client, sourceWithPlugins)
		require.NotEmpty(t, binary)

		// Load
		handle, err := dlopen.GetHandle([]string{binary})
		require.NoError(t, err)
		defer handle.Close()

		// Run
		for i := 0; i < 1000; i++ {
			checkExecute(t, client, binary, int64(i))
		}
	})
	t.Run("parallel parse", func(t *testing.T) {
		multithreadCheck(t, threads, func(t *testing.T, thread int) {
			for i := 0; i < 1000; i++ {
				checkParse(t, client, sourceSimple)
			}
		})
	})
	t.Run("parallel execution (multiple binaries)", func(t *testing.T) {
		var wg sync.WaitGroup
		wg.Add(threads)
		multithreadCheck(t, threads, func(t *testing.T, thread int) {
			binary := func() string {
				defer wg.Done()
				res := buildProgram(t, client, sourceSimple)
				require.NotEmpty(t, res)
				return res
			}()
			t.Logf("compiled: %d, %s", thread, binary)

			wg.Wait()
			t.Logf("run: %d", thread)
			for i := 0; i < 1000; i++ {
				checkExecute(t, client, binary, int64(i))
			}
			t.Logf("done: %d", thread)
		})
	})
	t.Run("parallel execution (multiple binaries, dlopen)", func(t *testing.T) {
		var wg sync.WaitGroup
		wg.Add(threads)
		multithreadCheck(t, threads, func(t *testing.T, thread int) {
			binary, handle := func() (string, *dlopen.LibHandle) {
				defer wg.Done()
				binary := buildProgram(t, client, sourceSimple)
				require.NotEmpty(t, binary)

				handle, err := dlopen.GetHandle([]string{binary})
				require.NoError(t, err)
				return binary, handle
			}()
			defer handle.Close()
			t.Logf("compiled: %d, %s", thread, binary)

			wg.Wait()
			t.Logf("run: %d", thread)
			for i := 0; i < 1000; i++ {
				checkExecute(t, client, binary, int64(i))
			}
			t.Logf("done: %d", thread)
		})
	})
	t.Run("parallel execution (multiple binaries, plugins)", func(t *testing.T) {
		var wg sync.WaitGroup
		wg.Add(threads)
		multithreadCheck(t, threads, func(t *testing.T, thread int) {
			binary := func() string {
				defer wg.Done()
				binary := buildProgram(t, client, sourceWithPlugins)
				require.NotEmpty(t, binary)
				return binary
			}()
			t.Logf("compiled: %d, %s", thread, binary)

			wg.Wait()
			t.Logf("run: %d", thread)
			for i := 0; i < 1000; i++ {
				checkExecute(t, client, binary, int64(i))
			}
			t.Logf("done: %d", thread)
		})
	})
	t.Run("parallel execution (single binary, copy)", func(t *testing.T) {
		var wg sync.WaitGroup
		wg.Add(threads)
		output := buildProgram(t, client, sourceSimple)
		require.NotEmpty(t, output)
		multithreadCheck(t, threads, func(t *testing.T, thread int) {
			binary, handle := func() (string, *dlopen.LibHandle) {
				defer wg.Done()
				res := path.Join(t.TempDir(), path.Base(output))

				file, err := os.ReadFile(output)
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(res, file, 0755))

				handle, err := dlopen.GetHandle([]string{res})
				require.NoError(t, err)
				return res, handle
			}()
			defer handle.Close()
			t.Logf("linked: %d, %s", thread, binary)

			wg.Wait()
			t.Logf("run: %d", thread)
			for i := 0; i < 1000; i++ {
				checkExecute(t, client, binary, int64(i))
			}
		})
	})
	t.Run("parallel execution (single binary, link)", func(t *testing.T) {
		var wg sync.WaitGroup
		wg.Add(threads)
		binary := buildProgram(t, client, sourceSimple)
		require.NotEmpty(t, binary)
		multithreadCheck(t, threads, func(t *testing.T, thread int) {
			binary := func() string {
				defer wg.Done()
				res := path.Join(t.TempDir(), path.Base(binary))
				require.NoError(t, os.Link(binary, res))
				return res
			}()
			t.Logf("linked: %d, %s", thread, binary)

			wg.Wait()
			t.Logf("run: %d", thread)
			for i := 0; i < 1000; i++ {
				checkExecute(t, client, binary, int64(i))
			}
		})
	})
	t.Run("parallel execution (single binary)", func(t *testing.T) {
		binary := buildProgram(t, client, sourceSimple)
		require.NotEmpty(t, binary)
		multithreadCheck(t, threads, func(t *testing.T, thread int) {
			for i := 0; i < 1000; i++ {
				checkExecute(t, client, binary, int64(i))
			}
		})
	})
	t.Run("parallel execution (single binary, dlopen)", func(t *testing.T) {
		binary := buildProgram(t, client, sourceSimple)
		require.NotEmpty(t, binary)
		multithreadCheck(t, threads, func(t *testing.T, thread int) {
			handle, err := dlopen.GetHandle([]string{binary})
			require.NoError(t, err)
			defer handle.Close()

			for i := 0; i < 1000; i++ {
				checkExecute(t, client, binary, int64(i))
			}
		})
	})
}

func TestFastEval(t *testing.T) {
	client := native.NewNativeServiceClient()

	threads := 10
	t.Run("serial execution", func(t *testing.T) {
		// Run
		for i := 0; i < 1000; i++ {
			checkFastEval(t, client, int64(i), sourceSimple)
		}
	})
	t.Run("parallel execution (simple)", func(t *testing.T) {
		multithreadCheck(t, threads, func(t *testing.T, thread int) {
			t.Logf("run: %d", thread)
			for i := 0; i < 1000; i++ {
				checkFastEval(t, client, int64(i), sourceSimple)
			}
			t.Logf("done: %d", thread)
		})
	})
	t.Run("parallel execution (regex)", func(t *testing.T) {
		multithreadCheck(t, threads, func(t *testing.T, thread int) {
			t.Logf("run: %d", thread)
			for i := 0; i < 1000; i++ {
				checkFastEval(t, client, int64(i), sourceRegex)
			}
			t.Logf("done: %d", thread)
		})
	})
	t.Run("parallel execution (plugins)", func(t *testing.T) {
		multithreadCheck(t, threads, func(t *testing.T, thread int) {
			t.Logf("run: %d", thread)
			for i := 0; i < 1000; i++ {
				checkFastEval(t, client, int64(i), sourceWithPlugins)
			}
			t.Logf("done: %d", thread)
		})
	})
}

func multithreadCheck(t *testing.T, threads int, check func(t *testing.T, thread int)) {
	var wg sync.WaitGroup
	wg.Add(threads)
	for i := 0; i < threads; i++ {
		thread := i
		go func() {
			defer wg.Done()
			check(t, thread)
		}()
	}
	wg.Wait()
}

func buildProgram(t *testing.T, client service.KclvmService, source string) string {
	tempDir := t.TempDir()
	result, err := client.BuildProgram(&gpyrpc.BuildProgram_Args{
		ExecArgs: &gpyrpc.ExecProgram_Args{
			KFilenameList: []string{"source.k"},
			KCodeList:     []string{source},
		},
		Output: path.Join(tempDir, "kcl"),
	})
	require.NoError(t, err, "BuildProgram returns error")
	return result.Path
}

func checkParse(t *testing.T, client service.KclvmService, source string) {
	_, err := client.ParseFile(&gpyrpc.ParseFile_Args{
		Source: source,
	})
	require.NoError(t, err, "ParseFile returns errors")
}

func checkExecute(t *testing.T, client service.KclvmService, output string, id int64) {
	value := strconv.FormatInt(id, 10)
	result, err := client.ExecArtifact(&gpyrpc.ExecArtifact_Args{
		ExecArgs: &gpyrpc.ExecProgram_Args{
			Args: []*gpyrpc.Argument{{
				Name:  "foo",
				Value: value,
			}},
		},
		Path: output,
	})
	require.NoError(t, err, "ExecArtifact returns errors")
	require.Empty(t, result.ErrMessage)

	var config struct {
		V int64 `json:"v"`
	}
	err = json.Unmarshal([]byte(result.JsonResult), &config)
	if err != nil {
		require.NoError(t, err, "Can't parse configuration: %v", result.JsonResult)
	}

	require.Equal(t, id, config.V)
}

func checkFastEval(t *testing.T, client service.KclvmService, id int64, source string) {
	value := strconv.FormatInt(id, 10)
	result, err := client.ExecProgram(&gpyrpc.ExecProgram_Args{
		KFilenameList: []string{"source.k"},
		KCodeList:     []string{source},
		Args: []*gpyrpc.Argument{{
			Name:  "foo",
			Value: value,
		}},
		FastEval: true,
	})
	require.NoError(t, err, "ExecProgram returns errors")
	require.Empty(t, result.ErrMessage)

	var config struct {
		V int64 `json:"v"`
	}
	err = json.Unmarshal([]byte(result.JsonResult), &config)
	if err != nil {
		require.NoError(t, err, "Can't parse configuration: %v", result.JsonResult)
	}

	require.Equal(t, id, config.V)
}
