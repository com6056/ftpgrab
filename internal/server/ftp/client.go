package ftp

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"time"

	"github.com/crazy-max/ftpgrab/v7/internal/config"
	"github.com/crazy-max/ftpgrab/v7/internal/logging"
	"github.com/crazy-max/ftpgrab/v7/internal/server"
	"github.com/crazy-max/ftpgrab/v7/pkg/utl"
	"github.com/jlaffaye/ftp"
	"github.com/rs/zerolog/log"
)

const (
	minSpeed         = 1024 * 1024      // Minimum acceptable download speed (1 MB/s)
	checkInterval    = 30 * time.Second // How often to check download speed
	minSlowChecks    = 2                // Number of consecutive slow checks before aborting
)

var ErrSlowTransfer = errors.New("transfer speed too slow, reconnecting")

// speedMonitor wraps an io.Reader to abort transfers that are too slow
type speedMonitor struct {
	reader          io.Reader
	lastCheck       time.Time
	lastBytes       int64
	totalBytes      int64
	minSpeed        int64
	interval        time.Duration
	consecutiveSlow int
}

func (s *speedMonitor) Read(p []byte) (n int, err error) {
	n, err = s.reader.Read(p)
	s.totalBytes += int64(n)

	now := time.Now()
	if now.Sub(s.lastCheck) >= s.interval {
		elapsed := now.Sub(s.lastCheck).Seconds()
		bytesSinceCheck := s.totalBytes - s.lastBytes

		if elapsed > 0 {
			speed := float64(bytesSinceCheck) / elapsed
			if speed < float64(s.minSpeed) && s.lastBytes > 0 {
				s.consecutiveSlow++
				log.Warn().
					Float64("speed_bps", speed).
					Int64("min_speed_bps", s.minSpeed).
					Int("consecutive_slow", s.consecutiveSlow).
					Msgf("Transfer speed below threshold (%d/%d)", s.consecutiveSlow, minSlowChecks)

				if s.consecutiveSlow >= minSlowChecks {
					return n, ErrSlowTransfer
				}
			} else {
				s.consecutiveSlow = 0
			}
		}

		s.lastCheck = now
		s.lastBytes = s.totalBytes
	}

	return n, err
}

// Client represents an active ftp object
type Client struct {
	*server.Client
	cfg *config.ServerFTP
	ftp *ftp.ServerConn
}

// New creates new ftp instance
func New(cfg *config.ServerFTP) (*server.Client, error) {
	var err error
	var client = &Client{cfg: cfg}

	ftpConfig := []ftp.DialOption{
		ftp.DialWithTimeout(*cfg.Timeout),
		ftp.DialWithDisabledEPSV(*cfg.DisableEPSV),
		ftp.DialWithDisabledUTF8(*cfg.DisableUTF8),
		ftp.DialWithDisabledMLSD(*cfg.DisableMLSD),
		ftp.DialWithDebugOutput(&logging.FtpWriter{
			Enabled: *cfg.LogTrace,
		}),
	}

	if *cfg.TLS {
		ftpConfig = append(ftpConfig, ftp.DialWithExplicitTLS(&tls.Config{
			ServerName:         cfg.Host,
			InsecureSkipVerify: *cfg.InsecureSkipVerify,
		}))
	}

	if client.ftp, err = ftp.Dial(fmt.Sprintf("%s:%d", cfg.Host, cfg.Port), ftpConfig...); err != nil {
		return nil, err
	}

	username, err := utl.GetSecret(cfg.Username, cfg.UsernameFile)
	if err != nil {
		log.Warn().Err(err).Msg("Cannot retrieve username secret for ftp server")
	}
	password, err := utl.GetSecret(cfg.Password, cfg.PasswordFile)
	if err != nil {
		log.Warn().Err(err).Msg("Cannot retrieve password secret for ftp server")
	}

	if len(username) > 0 {
		if err = client.ftp.Login(username, password); err != nil {
			return nil, err
		}
	}

	return &server.Client{Handler: client}, err
}

// Common return common configuration
func (c *Client) Common() config.ServerCommon {
	return config.ServerCommon{
		Host:    c.cfg.Host,
		Port:    c.cfg.Port,
		Sources: c.cfg.Sources,
	}
}

// ReadDir fetches the contents of a directory, returning a list of os.FileInfo's
func (c *Client) ReadDir(dir string) ([]os.FileInfo, error) {
	var files []*ftp.Entry

	if *c.cfg.EscapeRegexpMeta {
		dir = regexp.QuoteMeta(dir)
	}
	files, err := c.ftp.List(dir)
	if err != nil {
		return nil, err
	}

	var entries []os.FileInfo
	for _, file := range files {
		if file.Name == "." || file.Name == ".." {
			continue
		}
		var mode os.FileMode
		switch file.Type {
		case ftp.EntryTypeFolder:
			mode |= os.ModeDir
		case ftp.EntryTypeLink:
			mode |= os.ModeSymlink
		}
		fileInfo := &fileInfo{
			name:  file.Name,
			mode:  mode,
			mtime: file.Time,
			size:  int64(file.Size),
		}
		entries = append(entries, fileInfo)
	}

	return entries, nil
}

// Retrieve file "path" from server and write bytes to "dest".
func (c *Client) Retrieve(path string, dest io.Writer) error {
	resp, err := c.ftp.Retr(path)
	if err != nil {
		return err
	}
	defer resp.Close()

	monitor := &speedMonitor{
		reader:    resp,
		lastCheck: time.Now(),
		minSpeed:  minSpeed,
		interval:  checkInterval,
	}

	_, err = io.Copy(dest, monitor)
	return err
}

// Close closes ftp connection
func (c *Client) Close() error {
	return c.ftp.Quit()
}
