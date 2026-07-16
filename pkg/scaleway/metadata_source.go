package scaleway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/scaleway/scaleway-sdk-go/scw"

	"scaleway-sfs-subdir-csi/internal/strictjson"
)

const (
	metadataAPIURL         = "http://169.254.42.42/conf?format=json"
	cloudInitMetadataPath  = "/run/cloud-init/instance-data.json"
	metadataRequestTimeout = 10 * time.Second
	maxMetadataBytes       = 1 << 20
)

type localMetadataDocument struct {
	ID             string `json:"id"`
	CommercialType string `json:"commercial_type"`
	Location       struct {
		ZoneID string `json:"zone_id"`
	} `json:"location"`
}

type cloudInitDocument struct {
	DS struct {
		Metadata *localMetadataDocument `json:"meta_data"`
	} `json:"ds"`
}

// LocalMetadataSource reads the credential-free local Instance identity. It
// follows the official driver's source order but applies this driver's bounded
// and duplicate-safe trust contract.
type LocalMetadataSource struct {
	cloudInitPath string
	apiURL        string
	client        *http.Client
	timeout       time.Duration
}

// NewLocalMetadataSource constructs the fixed production cloud-init and
// link-local API readers. Proxy use and redirects are disabled so metadata can
// never be sent to a configured external proxy or redirect target.
func NewLocalMetadataSource() *LocalMetadataSource {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	return &LocalMetadataSource{
		cloudInitPath: cloudInitMetadataPath, apiURL: metadataAPIURL,
		client: &http.Client{
			Transport: transport,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return fmt.Errorf("metadata redirect is forbidden")
			},
		},
		timeout: metadataRequestTimeout,
	}
}

// Load tries cloud-init first and then the local API. Cancellation prevents a
// fallback request and no source error is interpreted as missing identity.
func (source *LocalMetadataSource) Load(ctx context.Context) (NodeIdentity, error) {
	if ctx == nil {
		return NodeIdentity{}, fmt.Errorf("local metadata context is nil")
	}
	if err := ctx.Err(); err != nil {
		return NodeIdentity{}, err
	}
	identity, cloudErr := source.loadCloudInit(ctx)
	if cloudErr == nil {
		return identity, nil
	}
	if err := ctx.Err(); err != nil {
		return NodeIdentity{}, err
	}
	identity, apiErr := source.loadAPI(ctx)
	if apiErr == nil {
		return identity, nil
	}
	return NodeIdentity{}, fmt.Errorf("local Scaleway metadata unavailable from cloud-init and API: %w", errors.Join(cloudErr, apiErr))
}

func (source *LocalMetadataSource) loadCloudInit(ctx context.Context) (identity NodeIdentity, returnErr error) {
	if err := ctx.Err(); err != nil {
		return NodeIdentity{}, err
	}
	info, err := os.Lstat(source.cloudInitPath)
	if err != nil {
		return NodeIdentity{}, fmt.Errorf("inspect cloud-init metadata: %w", err)
	}
	if !info.Mode().IsRegular() {
		return NodeIdentity{}, fmt.Errorf("cloud-init metadata path is not a regular file")
	}
	file, err := os.Open(source.cloudInitPath)
	if err != nil {
		return NodeIdentity{}, fmt.Errorf("open cloud-init metadata: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, file.Close()) }()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) || !opened.Mode().IsRegular() {
		return NodeIdentity{}, fmt.Errorf("cloud-init metadata changed during open")
	}
	data, err := readBoundedMetadata(file)
	if err != nil {
		return NodeIdentity{}, err
	}
	var document cloudInitDocument
	if err := strictjson.DecodeOpen(data, &document); err != nil {
		return NodeIdentity{}, fmt.Errorf("decode cloud-init metadata: %w", err)
	}
	if document.DS.Metadata == nil {
		return NodeIdentity{}, fmt.Errorf("cloud-init metadata document has no ds.meta_data")
	}
	return nodeIdentityFromMetadata(*document.DS.Metadata)
}

func (source *LocalMetadataSource) loadAPI(ctx context.Context) (identity NodeIdentity, returnErr error) {
	if source.client == nil {
		return NodeIdentity{}, fmt.Errorf("local metadata HTTP client is nil")
	}
	timeout := source.timeout
	if timeout <= 0 || timeout > metadataRequestTimeout {
		return NodeIdentity{}, fmt.Errorf("local metadata timeout is invalid")
	}
	requestContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(requestContext, http.MethodGet, source.apiURL, nil)
	if err != nil {
		return NodeIdentity{}, fmt.Errorf("construct local metadata request: %w", err)
	}
	response, err := source.client.Do(request)
	if err != nil {
		return NodeIdentity{}, fmt.Errorf("read local metadata API: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, response.Body.Close()) }()
	if response.StatusCode != http.StatusOK {
		return NodeIdentity{}, fmt.Errorf("local metadata API returned HTTP %d", response.StatusCode)
	}
	if response.ContentLength > maxMetadataBytes {
		return NodeIdentity{}, fmt.Errorf("local metadata API response exceeds %d bytes", maxMetadataBytes)
	}
	data, err := readBoundedMetadata(response.Body)
	if err != nil {
		return NodeIdentity{}, err
	}
	var document localMetadataDocument
	if err := strictjson.DecodeOpen(data, &document); err != nil {
		return NodeIdentity{}, fmt.Errorf("decode local metadata API response: %w", err)
	}
	return nodeIdentityFromMetadata(document)
}

func readBoundedMetadata(reader io.Reader) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(reader, maxMetadataBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read local metadata: %w", err)
	}
	if len(data) > maxMetadataBytes {
		return nil, fmt.Errorf("local metadata exceeds %d bytes", maxMetadataBytes)
	}
	return data, nil
}

func nodeIdentityFromMetadata(document localMetadataDocument) (NodeIdentity, error) {
	zone, err := scw.ParseZone(document.Location.ZoneID)
	if err != nil {
		return NodeIdentity{}, fmt.Errorf("metadata zone: %w", err)
	}
	region, err := zone.Region()
	if err != nil {
		return NodeIdentity{}, fmt.Errorf("metadata region: %w", err)
	}
	identity := NodeIdentity{
		InstanceID: document.ID, Zone: zone.String(), Region: region.String(), CommercialType: document.CommercialType,
	}
	if err := identity.Validate(); err != nil {
		return NodeIdentity{}, err
	}
	return identity, nil
}

var _ MetadataSource = (*LocalMetadataSource)(nil)
