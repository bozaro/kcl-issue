package kcl_lazy_issue

import (
	"bytes"
	"fmt"
	"os"
	"path"
	"testing"
	"text/template"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"kcl-lang.io/lib/go/api"
	"kcl-lang.io/lib/go/native"
)

const kcl_mod = `[package]
name = "__main__"
edition = "0.0.1"
version = "0.0.1"
`
const main_k = `
import big_unused_pkg

foo = 42
`

func TestLazy1(t *testing.T) {
	runLazyTest(t, `
schema Config:
    name: str
    foo: str
    bar: str
    baz: str

schema Registry:
    [str]: Config

registry = Registry {
{{- range $i := until 1000 }}
    test_{{ $i }} = Config {
        name: "test-{{ $i }}"
        foo: "foo-{{ $i }}"
        bar: "bar-{{ $i }}"
        baz: "baz-{{ $i }}"
    }
{{- end }}
}
`)
}

func TestLazy2(t *testing.T) {
	runLazyTest(t, `
schema Config:
    name: str
    foo: str
    bar: str
    baz: str

schema Registry:
    [str]: Config

{{- range $i := until 1000 }}
test_{{ $i }} = Config {
	name: "test-{{ $i }}"
	foo: "foo-{{ $i }}"
	bar: "bar-{{ $i }}"
	baz: "baz-{{ $i }}"
}
{{- end }}
`)
}

func runLazyTest(t *testing.T, bigPkgTemplate string) {
	dir := t.TempDir()
	generateSources(t, path.Join(dir, "kcl.mod"), kcl_mod)
	generateSources(t, path.Join(dir, "main.k"), main_k)
	generateSources(t, path.Join(dir, "big_unused_pkg/data.k"), bigPkgTemplate)
	client := native.NewNativeServiceClient()
	for pass := 0; pass < 3; pass++ {
		t.Run(fmt.Sprintf("pass_%d", pass), func(t *testing.T) {
			out, err := client.ExecProgram(&api.ExecProgram_Args{
				WorkDir:           dir,
				KFilenameList:     []string{path.Join(dir, "main.k")},
				DisableYamlResult: true,
				FastEval:          true,
			})
			require.NoError(t, err)
			assert.Equal(t, `{"foo": 42}`, out.JsonResult)
		})
	}

}

func generateSources(t *testing.T, fileName string, templateContent string) {
	compiled, err := template.New("").Funcs(map[string]any{
		"until": until,
	}).Parse(templateContent)
	require.NoError(t, err)

	var data bytes.Buffer
	require.NoError(t, compiled.Execute(&data, nil))

	os.MkdirAll(path.Dir(fileName), 0755)
	require.NoError(t, os.WriteFile(fileName, data.Bytes(), 0644))
}

func until(count int) []int {
	v := make([]int, count)
	for i := 0; i < count; i++ {
		v[i] = i
	}
	return v
}
