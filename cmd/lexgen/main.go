// MIT License Bluesky Social
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package main

import (
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/bluesky-social/indigo/lex"
	"github.com/urfave/cli/v2"
)

func findSchemas(dir string, out []string) ([]string, error) {
	err := filepath.Walk(dir, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		if strings.HasSuffix(path, ".json") {
			out = append(out, path)
		}

		return nil
	})
	if err != nil {
		return out, err
	}

	return out, nil

}

// for direct .json lexicon files or directories containing lexicon .json files, get one flat list of all paths to .json files
func expandArgs(args []string) ([]string, error) {
	var out []string
	for _, a := range args {
		st, err := os.Stat(a)
		if err != nil {
			return nil, err
		}
		if st.IsDir() {
			out, err = findSchemas(a, out)
			if err != nil {
				return nil, err
			}
		} else if strings.HasSuffix(a, ".json") {
			out = append(out, a)
		}
	}

	return out, nil
}

func main() {
	app := cli.NewApp()

	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name: "outdir",
		},
		&cli.BoolFlag{
			Name: "gen-server",
		},
		&cli.BoolFlag{
			Name: "gen-handlers",
		},
		&cli.StringSliceFlag{
			Name: "types-import",
		},
		&cli.StringSliceFlag{
			Name: "external-lexicons",
		},
		&cli.StringFlag{
			Name:  "package",
			Value: "schemagen",
		},
		&cli.StringFlag{
			Name:  "build",
			Value: "",
		},
		&cli.StringFlag{
			Name:  "build-file",
			Value: "",
		},
	}
	app.Action = func(cctx *cli.Context) error {
		paths, err := expandArgs(cctx.Args().Slice())
		if err != nil {
			return err
		}

		var schemas []*lex.Schema
		for _, arg := range paths {
			if strings.HasSuffix(arg, "com/atproto/temp/importRepo.json") {
				fmt.Printf("skipping schema: %s\n", arg)
				continue
			}
			s, err := lex.ReadSchema(arg)
			if err != nil {
				return fmt.Errorf("failed to read file %q: %w", arg, err)
			}

			schemas = append(schemas, s)
		}

		externalPaths, err := expandArgs(cctx.StringSlice("external-lexicons"))
		if err != nil {
			return err
		}
		var externalSchemas []*lex.Schema
		for _, arg := range externalPaths {
			s, err := lex.ReadSchema(arg)
			if err != nil {
				return fmt.Errorf("failed to read file %q: %w", arg, err)
			}

			externalSchemas = append(externalSchemas, s)
		}

		buildLiteral := cctx.String("build")
		buildPath := cctx.String("build-file")
		var packages []lex.Package
		if buildLiteral != "" {
			if buildPath != "" {
				return errors.New("must not set both --build and --build-file")
			}
			packages, err = lex.ParsePackages([]byte(buildLiteral))
			if err != nil {
				return fmt.Errorf("--build error, %w", err)
			}
			if len(packages) == 0 {
				return errors.New("--build must specify at least one Package{}")
			}
		} else if buildPath != "" {
			blob, err := os.ReadFile(buildPath)
			if err != nil {
				return fmt.Errorf("--build-file error, %w", err)
			}
			packages, err = lex.ParsePackages(blob)
			if err != nil {
				return fmt.Errorf("--build-file error, %w", err)
			}
			if len(packages) == 0 {
				return errors.New("--build-file must specify at least one Package{}")
			}
		} else {
			return errors.New("need exactly one of --build or --build-file")
		}

		if cctx.Bool("gen-server") {
			pkgname := cctx.String("package")
			outdir := cctx.String("outdir")
			if outdir == "" {
				return fmt.Errorf("must specify output directory (--outdir)")
			}
			defmap := lex.BuildExtDefMap(append(schemas, externalSchemas...), packages)
			_ = defmap

			paths := cctx.StringSlice("types-import")
			importmap := make(map[string]string)
			for _, p := range paths {
				parts := strings.Split(p, ":")
				importmap[parts[0]] = parts[1]
			}

			handlers := cctx.Bool("gen-handlers")

			if err := lex.CreateHandlerStub(pkgname, importmap, outdir, schemas, handlers); err != nil {
				return err
			}

		} else {
			return lex.Run(schemas, externalSchemas, packages)
		}

		return nil
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
