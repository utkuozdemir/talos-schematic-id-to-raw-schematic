package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/klauspost/compress/zstd"
	"github.com/siderolabs/talos/pkg/machinery/extensions"
	"github.com/u-root/u-root/pkg/cpio"
	"github.com/ulikunitz/xz"
	"gopkg.in/yaml.v3"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("failed to run: %v", err)
	}
}

func run() error {
	if len(os.Args) < 2 {
		return fmt.Errorf("usage: %s <schematic-id>", os.Args[0])
	}

	schematicID := os.Args[1]
	talosVersion := "v1.10.6"
	imageFactoryBaseURL := "https://factory.talos.dev"

	homeDir, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("failed to get user home dir: %v", err)
	}

	cacheDir := filepath.Join(homeDir, ".talos-schematic-id-to-raw-schematic-cache")
	if err = os.MkdirAll(cacheDir, 0o700); err != nil {
		return fmt.Errorf("failed to create cache dir %q: %v", cacheDir, err)
	}

	filePath := filepath.Join(cacheDir, fmt.Sprintf("%s-initramfs-amd64-%s.xz", talosVersion, schematicID))

	_, err = os.Stat(filePath)
	if err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("failed to stat file %q: %v", filePath, err)
		}

		// download the file
		initramfsURL := fmt.Sprintf("%s/image/%s/%s/initramfs-amd64.xz", imageFactoryBaseURL, schematicID, talosVersion)

		log.Printf("downloading initramfs from %q to %q", initramfsURL, filePath)

		out, err := os.Create(filePath)
		if err != nil {
			return fmt.Errorf("failed to create file %q: %v", filePath, err)
		}

		defer out.Close()

		resp, err := http.Get(initramfsURL)
		if err != nil {
			return fmt.Errorf("failed to download initramfs from %q: %v", initramfsURL, err)
		}

		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("failed to download initramfs from %q: status code %d", initramfsURL, resp.StatusCode)
		}

		if _, err = io.Copy(out, resp.Body); err != nil {
			return fmt.Errorf("failed to write initramfs to %q: %v", filePath, err)
		}
	} else {
		log.Printf("using cached initramfs at %q", filePath)
	}

	f, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open %q: %v", filePath, err)
	}

	in := bufio.NewReader(f)

	r := &discarder{r: in}

	cpioR := cpio.Newc.Reader(r)

	for {
		magic, err := in.Peek(4)
		if err != nil {
			return err
		}

		var rdr io.Reader = in

		switch {
		case bytes.Equal(magic, []byte{0xfd, '7', 'z', 'X'}):
			// xz-compressed
			if rdr, err = xz.NewReader(in); err != nil {
				return err
			}
		case bytes.Equal(magic, []byte{0x28, 0xb5, 0x2f, 0xfd}):
			// zstd-compressed
			if rdr, err = zstd.NewReader(in); err != nil {
				return err
			}
		}

		in = bufio.NewReader(rdr)

		r = &discarder{r: in}
		cpioR = cpio.Newc.Reader(r)

		for {
			record, err := cpioR.ReadRecord()
			if err == io.EOF {
				break
			}

			if err != nil {
				return err
			}

			log.Printf("found record: %q", record.Name)

			if record.Name == "extensions.yaml" {
				var extensionConfig extensions.Config

				yamlReader := record.ReaderAt.(*io.SectionReader)

				if err = yaml.NewDecoder(yamlReader).Decode(&extensionConfig); err != nil {
					return err
				}

				lastLayer := extensionConfig.Layers[len(extensionConfig.Layers)-1]

				log.Printf("raw schematic:\n%s", lastLayer.Metadata.ExtraInfo)
				log.Printf("done, exiting")

				return nil
			}
		}

		if err = eatPadding(in); err != nil {
			return err
		}
	}
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
		return 0, fmt.Errorf("negative seek on discarder not allowed")
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
		if err == io.EOF {
			return nil
		}

		if err != nil {
			return err
		}

		if b != 0 {
			if err = in.UnreadByte(); err != nil {
				return err
			}

			return nil
		}
	}
}
