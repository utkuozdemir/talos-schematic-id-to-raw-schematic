package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/siderolabs/talos/pkg/machinery/extensions"
	"github.com/u-root/u-root/pkg/cpio"
	"github.com/ulikunitz/xz"
	"gopkg.in/yaml.v3"
)

const (
	defaultTalosVersion    = "v1.10.6"
	defaultFactoryBaseURL  = "https://factory.talos.dev"
	defaultCacheDirName    = ".talos-schematic-id-to-raw-schematic-cache"
	initramfsFilenameTmpl  = "%s-initramfs-amd64-%s.xz" // talosVersion, schematicID
	initramfsURLTmpl       = "%s/image/%s/%s/initramfs-amd64.xz"
	extensionsYAMLFileName = "extensions.yaml"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("failed to run: %v", err)
	}
}

func run() error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cfg, err := buildConfig()
	if err != nil {
		return err
	}

	if err = os.MkdirAll(cfg.CacheDir, 0o700); err != nil {
		return fmt.Errorf("failed to create cache dir %q: %w", cfg.CacheDir, err)
	}

	localPath := cfg.cacheFilePath()
	if err = ensureInitramfsCached(ctx, localPath, cfg.initramfsURL()); err != nil {
		return err
	}

	initramfsFile, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("failed to open %q: %w", localPath, err)
	}

	defer func() {
		if closeErr := initramfsFile.Close(); closeErr != nil {
			log.Printf("warning: failed to close file %q: %v", localPath, closeErr)
		}
	}()

	raw, err := extractRawSchematic(bufio.NewReader(initramfsFile))
	if err != nil {
		return err
	}

	log.Printf("raw schematic:\n%s", raw)
	log.Printf("done, exiting")

	return nil
}

type config struct {
	SchematicID  string
	TalosVersion string
	BaseURL      string
	CacheDir     string
}

func buildConfig() (*config, error) {
	if len(os.Args) < 2 {
		return nil, fmt.Errorf("usage: %s <schematic-id>", os.Args[0])
	}

	sid := os.Args[1]

	homeDir, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("failed to get user home dir: %w", err)
	}

	return &config{
		SchematicID:  sid,
		TalosVersion: defaultTalosVersion,
		BaseURL:      defaultFactoryBaseURL,
		CacheDir:     filepath.Join(homeDir, defaultCacheDirName),
	}, nil
}

func (c *config) cacheFilePath() string {
	return filepath.Join(
		c.CacheDir,
		fmt.Sprintf(initramfsFilenameTmpl, c.TalosVersion, c.SchematicID),
	)
}

func (c *config) initramfsURL() string {
	return fmt.Sprintf(initramfsURLTmpl, c.BaseURL, c.SchematicID, c.TalosVersion)
}

//nolint:cyclop
func ensureInitramfsCached(ctx context.Context, path, url string) error {
	if _, err := os.Stat(path); err == nil {
		log.Printf("using cached initramfs at %q", path)

		return nil
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}

	tmp, err := os.CreateTemp(filepath.Dir(path), ".partial-*")
	if err != nil {
		return fmt.Errorf("create temp: %w", err)
	}

	defer func() { _ = os.Remove(tmp.Name()); _ = tmp.Close() }()

	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("new request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("download: %w", err)
	}

	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			log.Printf("warning: failed to close response body: %v", closeErr)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download status %d", resp.StatusCode)
	}

	if _, err = io.Copy(tmp, resp.Body); err != nil {
		return fmt.Errorf("write: %w", err)
	}

	if err = tmp.Sync(); err != nil {
		return fmt.Errorf("fsync: %w", err)
	}

	if err = tmp.Close(); err != nil {
		return fmt.Errorf("close tmp: %w", err)
	}

	return os.Rename(tmp.Name(), path)
}

func extractRawSchematic(reader *bufio.Reader) (string, error) {
	var closeFuncs []func()

	defer func() {
		for _, c := range closeFuncs {
			c()
		}
	}()

	for {
		decReader, closeFunc, err := decompressingReadCloser(reader)
		if err != nil {
			return "", err
		}

		closeFuncs = append(closeFuncs, closeFunc)

		reader = bufio.NewReader(decReader)

		d := &discarder{r: reader}
		cpioReader := cpio.Newc.Reader(d)

		var rec cpio.Record

		for {
			if rec, err = cpioReader.ReadRecord(); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}

				return "", err
			}

			log.Printf("found record: %q", rec.Name)

			if rec.Name == extensionsYAMLFileName {
				return parseRawFromExtensions(rec.ReaderAt)
			}
		}

		if err = eatPadding(reader); err != nil {
			return "", err
		}
	}
}

func decompressingReadCloser(in *bufio.Reader) (rdr io.Reader, closeFunc func(), err error) {
	magic, err := in.Peek(4)
	if err != nil {
		return nil, nil, err
	}

	switch {
	case bytes.Equal(magic, []byte{0xfd, '7', 'z', 'X'}): // xz
		var reader io.Reader

		if reader, err = xz.NewReader(in); err != nil {
			return nil, nil, err
		}

		return reader, func() {}, nil
	case bytes.Equal(magic, []byte{0x28, 0xb5, 0x2f, 0xfd}): // zstd
		var decoder *zstd.Decoder

		if decoder, err = zstd.NewReader(in); err != nil {
			return nil, nil, err
		}

		return decoder, decoder.Close, nil
	default:
		return in, func() {}, nil // return the original reader
	}
}

func parseRawFromExtensions(readerAt io.ReaderAt) (string, error) {
	sectionReader, ok := readerAt.(*io.SectionReader)
	if !ok {
		return "", fmt.Errorf("unexpected ReaderAt type %T; want *io.SectionReader", readerAt)
	}

	var extensionsConfig extensions.Config

	if err := yaml.NewDecoder(sectionReader).Decode(&extensionsConfig); err != nil {
		return "", err
	}

	if len(extensionsConfig.Layers) == 0 {
		return "", errors.New("extensions.yaml has no layers")
	}

	last := extensionsConfig.Layers[len(extensionsConfig.Layers)-1]

	return last.Metadata.ExtraInfo, nil
}

// discarder is used to implement ReadAt from a Reader
// by reading, and discarding, data until the offset
// is reached. it can only go forward. it is designed
// for pipe-like files.
type discarder struct {
	r   io.Reader
	pos int64
}

// ReadAt implements ReadAt for a discarder.
// It is an error for the offset to be negative.
func (r *discarder) ReadAt(p []byte, off int64) (int, error) {
	if off-r.pos < 0 {
		return 0, errors.New("negative seek on discarder not allowed")
	}

	if off != r.pos {
		i, err := io.Copy(io.Discard, io.LimitReader(r.r, off-r.pos))
		if err != nil || i != off-r.pos {
			return 0, err
		}

		r.pos += i
	}

	n, err := io.ReadFull(r.r, p)
	if err != nil {
		return n, err
	}

	r.pos += int64(n)

	return n, err
}

var _ io.ReaderAt = &discarder{}

func eatPadding(in io.ByteScanner) error {
	for {
		b, err := in.ReadByte()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}

			return err
		}

		if b != 0 {
			return in.UnreadByte()
		}
	}
}
