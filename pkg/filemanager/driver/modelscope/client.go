package modelscope

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cloudreve/Cloudreve/v4/pkg/logging"
)

// inlineLimit is the largest object ModelScope stores as an inline, base64
// encoded repository file rather than an LFS blob. Larger objects must go
// through the LFS batch API.
const inlineLimit int64 = 5 * 1024 * 1024

const (
	// maxEnvelopeSize bounds an API envelope body. Envelopes are small JSON
	// documents; the limit keeps a wrong or hostile path from streaming an
	// arbitrary repository file into memory.
	maxEnvelopeSize int64 = 2 << 20
	// maxRedirects bounds the redirect chain followed when reading an object.
	maxRedirects = 5
	// upstreamErrorBodyLimit bounds how much of a failed response body is read
	// while building an error, so an error path cannot stream a large body.
	upstreamErrorBodyLimit int64 = 4 << 10
	// upstreamErrorDetailLimit bounds the body excerpt echoed into an error.
	upstreamErrorDetailLimit = 512
)

// storageHostSuffixes is the allowlist of hosts a ModelScope redirect may point
// at. A redirect to any other host is rejected so a misconfigured or hostile
// endpoint cannot turn object reads into requests against arbitrary hosts.
var storageHostSuffixes = []string{".modelscope.cn", ".aliyuncs.com", ".amazonaws.com"}

// hashRegexp matches a lowercase hex SHA-256 digest.
var hashRegexp = regexp.MustCompile(`^[a-f0-9]{64}$`)

// repoIDRegexp matches a repository id in owner/name form.
var repoIDRegexp = regexp.MustCompile(`^[A-Za-z0-9_-]+/[A-Za-z0-9_.-]+$`)

// revisionRegexp matches a safe repository revision name.
var revisionRegexp = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)

// ErrUpstream is returned for any ModelScope request that failed or answered
// with an envelope that does not confirm success. Callers test it with
// errors.Is; the concrete error wraps it so the cause survives into the log.
var ErrUpstream = errors.New("ModelScope request failed")

// upstreamError adds the failing call to ErrUpstream. Without it every failure
// is reported as the same opaque string, which makes a repository that is
// missing, unreadable or out of space indistinguishable from a network fault.
func upstreamError(operation string, cause error) error {
	if cause == nil {
		return fmt.Errorf("%w: %s", ErrUpstream, operation)
	}
	return fmt.Errorf("%w: %s: %v", ErrUpstream, operation, cause)
}

// upstreamStatus reports a non-success HTTP answer with its code and a bounded
// excerpt of the body, which is where ModelScope explains what went wrong.
func upstreamStatus(operation string, resp *http.Response) error {
	body, err := io.ReadAll(io.LimitReader(resp.Body, upstreamErrorBodyLimit))
	if err != nil {
		return upstreamError(operation, fmt.Errorf("HTTP %d", resp.StatusCode))
	}

	detail := strings.Join(strings.Fields(string(body)), " ")
	if len(detail) > upstreamErrorDetailLimit {
		detail = detail[:upstreamErrorDetailLimit] + "..."
	}

	if detail == "" {
		return upstreamError(operation, fmt.Errorf("HTTP %d", resp.StatusCode))
	}

	return upstreamError(operation, fmt.Errorf("HTTP %d: %s", resp.StatusCode, detail))
}

// Client speaks the subset of the ModelScope repository API needed to store
// and retrieve content-addressed blobs.
type Client struct {
	endpoint string
	token    string
	repo     string
	repoType string
	revision string

	// api performs authenticated requests against the ModelScope origin and
	// never follows redirects, so the credential can only ever be sent to the
	// configured endpoint.
	api *http.Client
	// storage fetches redirect targets. It carries no credential and refuses to
	// connect to non-public addresses.
	storage *http.Client

	l logging.Logger
}

// NewClient constructs a ModelScope API client. endpoint is the site origin,
// repo is the "owner/name" repository id and token is the access token.
func NewClient(endpoint, token, repo, repoType, revision string, l logging.Logger) (*Client, error) {
	if token == "" || strings.ContainsAny(token, "\r\n;") {
		return nil, errors.New("ModelScope access token is required and must be a single token")
	}

	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" ||
		(u.Path != "" && u.Path != "/") {
		return nil, errors.New("ModelScope endpoint must be an HTTPS origin")
	}

	if !repoIDRegexp.MatchString(repo) {
		return nil, errors.New("ModelScope repository id must be in owner/name form")
	}

	if repoType != "models" && repoType != "datasets" {
		return nil, errors.New("ModelScope repository type must be models or datasets")
	}

	if revision == "" || !revisionRegexp.MatchString(revision) || revision == "." || revision == ".." {
		return nil, errors.New("invalid ModelScope revision")
	}

	apiTransport := http.DefaultTransport.(*http.Transport).Clone()
	apiTransport.ResponseHeaderTimeout = 60 * time.Second

	storageTransport := apiTransport.Clone()
	storageTransport.Proxy = nil
	storageTransport.DialContext = dialPublicOnly

	return &Client{
		endpoint: strings.TrimRight(endpoint, "/"),
		token:    token,
		repo:     repo,
		repoType: repoType,
		revision: revision,
		api: &http.Client{
			Transport: apiTransport,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		storage: &http.Client{
			Transport: storageTransport,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		l: l,
	}, nil
}

// dialPublicOnly resolves the target host and refuses to connect unless every
// resolved address is a public one, validating the destination at connection
// time rather than relying on a pre-flight check.
func dialPublicOnly(ctx context.Context, network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}

	ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}

	if len(ips) == 0 {
		return nil, upstreamError("resolve storage host "+host, errors.New("no address found"))
	}

	for _, ip := range ips {
		if !isPublicIP(ip.IP) {
			// Refusing a private address is a security decision, so the reason
			// must be visible rather than looking like a generic network fault.
			return nil, upstreamError("connect storage host "+host,
				fmt.Errorf("refusing non-public address %s", ip.IP))
		}
	}

	return (&net.Dialer{Timeout: 30 * time.Second}).DialContext(ctx, network, net.JoinHostPort(ips[0].IP.String(), port))
}

func isPublicIP(ip net.IP) bool {
	if !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
		return false
	}

	_, cgnat, _ := net.ParseCIDR("100.64.0.0/10")
	return !cgnat.Contains(ip)
}

// safeStorageURL reports whether raw is an HTTPS URL on an allowlisted storage
// host that is safe to fetch without a credential.
func safeStorageURL(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.User != nil || u.Hostname() == "" ||
		(u.Port() != "" && u.Port() != "443") || u.Fragment != "" {
		return false
	}

	host := strings.ToLower(u.Hostname())
	if net.ParseIP(host) != nil {
		return false
	}

	for _, suffix := range storageHostSuffixes {
		if strings.HasSuffix(host, suffix) {
			return true
		}
	}

	return false
}

// isAPIOrigin reports whether host is the configured ModelScope API origin.
// That origin carries the repository credential and is never a valid upload or
// redirect target, regardless of the storage host allowlist.
func isAPIOrigin(host string) bool {
	host = strings.ToLower(host)
	return host == "modelscope.cn" || strings.HasSuffix(host, ".modelscope.cn")
}

// uploadTarget classifies a URL offered as an upload destination:
//
//   - authenticated: the canonical ModelScope LFS route, which is the only
//     destination that may receive the repository credential.
//   - valid: an acceptable destination at all. A rewritten or non-canonical
//     target is refused rather than followed.
func (c *Client) uploadTarget(raw, hash string, size int64) (authenticated, valid bool) {
	if !safeStorageURL(raw) || size < 0 || !hashRegexp.MatchString(hash) {
		return false, false
	}

	if raw == c.lfsUploadRoute(hash, size) {
		return true, true
	}

	if u, err := url.Parse(raw); err != nil || isAPIOrigin(u.Hostname()) {
		return false, false
	}

	return false, true
}

// auth applies the repository credential to an API request.
func (c *Client) auth(req *http.Request) {
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.AddCookie(&http.Cookie{Name: "m_session_id", Value: c.token})
}

func (c *Client) apiURL(endpoint string) string {
	return c.endpoint + "/api/v1/" + endpoint
}

// envelope is the ModelScope response wrapper. A mutation is confirmed only by
// an explicit, non-conflicting success envelope.
type envelope struct {
	Code    json.RawMessage `json:"Code"`
	Success *bool           `json:"Success"`
	Data    json.RawMessage `json:"Data"`
}

// decodeEnvelope validates the wrapper and returns the Data section.
func decodeEnvelope(body []byte) (json.RawMessage, error) {
	var env envelope
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, fmt.Errorf("response is not a JSON envelope: %w", err)
	}

	code := strings.Trim(string(env.Code), `"`)
	if code != "" && code != "200" {
		return nil, fmt.Errorf("upstream returned code %s", code)
	}

	if env.Success != nil && !*env.Success {
		return nil, errors.New("upstream reported failure")
	}

	return env.Data, nil
}

// do performs a JSON API request and decodes the envelope's data section into out.
func (c *Client) do(ctx context.Context, method, endpoint string, payload any, out any) error {
	var body io.Reader
	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			return upstreamError("encode request "+endpoint, err)
		}

		body = bytes.NewReader(raw)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.apiURL(endpoint), body)
	if err != nil {
		return upstreamError("build request "+endpoint, err)
	}

	c.auth(req)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.api.Do(req)
	if err != nil {
		return upstreamError("request "+endpoint, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return upstreamStatus(endpoint, resp)
	}

	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxEnvelopeSize+1))
	if err != nil {
		return upstreamError("read response of "+endpoint, err)
	}
	if int64(len(raw)) > maxEnvelopeSize {
		return upstreamError("read response of "+endpoint, errors.New("response exceeds size limit"))
	}

	data, err := decodeEnvelope(raw)
	if err != nil {
		return upstreamError(endpoint, err)
	}

	if out != nil && len(data) > 0 {
		if err := json.Unmarshal(data, out); err != nil {
			return upstreamError("decode response of "+endpoint, err)
		}
	}

	return nil
}

// lfsUploadRoute is the exact ModelScope LFS route that may receive the
// repository credential. Allowlisting a storage host is never by itself
// authority to disclose the token.
func (c *Client) lfsUploadRoute(hash string, size int64) string {
	return "https://lfs.modelscope.cn/api/v1/repos/" + c.repoType + "/" + c.repo + "/blobs/" +
		hash + "/" + strconv.FormatInt(size, 10)
}

// Validate asks the LFS batch endpoint for an upload target. It returns an
// empty target when the blob is already stored, which lets the caller skip the
// upload and only commit the pointer.
func (c *Client) Validate(ctx context.Context, hash string, size int64) (string, error) {
	if !hashRegexp.MatchString(hash) {
		return "", upstreamError("request upload target", errors.New("object digest is not a SHA-256"))
	}

	var data struct {
		Objects []struct {
			OID     string          `json:"oid"`
			Size    int64           `json:"size"`
			Error   json.RawMessage `json:"error"`
			Actions map[string]struct {
				Href             string            `json:"href"`
				Header           map[string]string `json:"header"`
				UploadHeader     map[string]string `json:"upload_header"`
				UploadParameters map[string]string `json:"upload_parameters"`
				Offset           int64             `json:"offset"`
			} `json:"actions"`
		} `json:"objects"`
	}

	payload := map[string]any{
		"operation": "upload",
		"objects":   []any{map[string]any{"oid": hash, "size": size}},
	}

	if err := c.do(ctx, http.MethodPost, "repos/"+c.repoType+"/"+c.repo+"/info/lfs/objects/batch", payload, &data); err != nil {
		return "", err
	}

	for _, object := range data.Objects {
		if object.OID != hash {
			continue
		}

		if len(object.Error) > 0 && string(object.Error) != "null" {
			return "", upstreamError("request upload target", fmt.Errorf("upstream reported %s", string(object.Error)))
		}

		if object.Size != 0 && object.Size != size {
			return "", upstreamError("request upload target",
				fmt.Errorf("upstream reports size %d, expected %d", object.Size, size))
		}

		action, ok := object.Actions["upload"]
		if !ok {
			return "", nil
		}

		// The action must be an acceptable destination, carry no extra headers or
		// parameters and start at offset zero. Anything else is rejected rather
		// than followed with the credential attached.
		if _, valid := c.uploadTarget(action.Href, hash, size); !valid ||
			len(action.Header) > 0 || len(action.UploadParameters) > 0 || action.Offset != 0 {
			return "", upstreamError("request upload target",
				fmt.Errorf("upstream offered an unacceptable destination %q", action.Href))
		}

		if len(action.UploadHeader) > 0 &&
			(len(action.UploadHeader) != 1 || action.UploadHeader["Range"] != "bytes=0-") {
			return "", upstreamError("request upload target",
				fmt.Errorf("upstream requested unsupported headers %v", action.UploadHeader))
		}

		return action.Href, nil
	}

	// ModelScope omits the requested object from the batch reply when the blob is
	// already stored: an oid absent from objects means "exists".
	return "", nil
}

// Put streams an object to the upload target returned by Validate.
func (c *Client) Put(ctx context.Context, target, hash string, r io.Reader, size int64) error {
	authenticated, valid := c.uploadTarget(target, hash, size)
	if !valid {
		return upstreamError("resolve upload target", errors.New("upstream offered an unacceptable destination"))
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, target, r)
	if err != nil {
		return upstreamError("build blob upload request", err)
	}

	if authenticated {
		c.auth(req)
	}

	// r is expected to be an *os.File or similar; the length is known and set
	// explicitly so the request is not chunked.
	req.ContentLength = size
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := c.storage.Do(req)
	if err != nil {
		return upstreamError(fmt.Sprintf("upload blob %s (%d bytes)", hash, size), err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return upstreamStatus(fmt.Sprintf("upload blob %s", hash), resp)
	}

	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxEnvelopeSize+1))
	if err != nil {
		return upstreamError("read blob upload response", err)
	}
	if int64(len(raw)) > maxEnvelopeSize {
		return upstreamError("read blob upload response", errors.New("response exceeds size limit"))
	}

	if authenticated {
		if _, err := decodeEnvelope(raw); err != nil {
			return upstreamError(fmt.Sprintf("confirm blob upload %s", hash), err)
		}
		return nil
	}

	// Unauthenticated storage endpoints answer with an empty body or their own
	// envelope; only a definitively failed envelope is treated as an error.
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil
	}

	var env envelope
	if json.Unmarshal(raw, &env) == nil {
		if code := strings.Trim(string(env.Code), `"`); code != "" && code != "200" {
			return upstreamError(fmt.Sprintf("confirm blob upload %s", hash),
				fmt.Errorf("upstream returned code %s", code))
		}
	}

	return nil
}

// Commit writes blob pointers and inline documents in a single repository
// commit. Each entry of actions is created atomically with the rest.
func (c *Client) Commit(ctx context.Context, actions []map[string]any) error {
	if len(actions) == 0 {
		return nil
	}

	payload := map[string]any{
		"commit_message": "Cloudreve store",
		"actions":        actions,
	}

	return c.do(ctx, http.MethodPost, "repos/"+c.repoType+"/"+c.repo+"/commit/"+url.PathEscape(c.revision), payload, nil)
}

// blobAction describes an LFS blob pointer to commit.
func blobAction(objectPath, hash string, size int64) map[string]any {
	return map[string]any{
		"action":   "create",
		"path":     objectPath,
		"type":     "lfs",
		"size":     size,
		"sha256":   hash,
		"content":  "",
		"encoding": "",
	}
}

// inlineAction describes a small file committed with its content inline.
func inlineAction(objectPath string, content []byte) map[string]any {
	return map[string]any{
		"action":   "create",
		"path":     objectPath,
		"type":     "normal",
		"size":     len(content),
		"sha256":   "",
		"content":  base64.StdEncoding.EncodeToString(content),
		"encoding": "base64",
	}
}

// Exists reports whether a repository file is present in the revision.
func (c *Client) Exists(ctx context.Context, objectPath string) (bool, error) {
	resp, err := c.startRead(ctx, objectPath)
	if err != nil {
		return false, err
	}

	defer resp.Body.Close()

	switch {
	case resp.StatusCode == http.StatusNotFound:
		return false, nil
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return true, nil
	case resp.StatusCode >= 300 && resp.StatusCode < 400:
		return true, nil
	default:
		// A rejected read is the first thing an upload hits, so the status and
		// upstream message are what tell an operator whether the repository is
		// missing, the token lacks write access, or the revision is wrong.
		return false, upstreamStatus("check object "+objectPath, resp)
	}
}

// startRead issues the authenticated repository file request without following
// any redirect, so the credential can never leave the API origin.
//
// The file read route does NOT carry the "repos/" segment that the LFS and
// commit routes use: it is "<repoType>/<repo>/repo". Requesting the "repos/"
// form is answered by an edge mirror with 421 "mirror self-forwarded loop
// detected" rather than by the API, so the two must not be conflated.
func (c *Client) startRead(ctx context.Context, objectPath string) (*http.Response, error) {
	query := url.Values{
		"Revision": {c.revision},
		"FilePath": {objectPath},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		c.apiURL(c.repoType+"/"+c.repo+"/repo")+"?"+query.Encode(), nil)
	if err != nil {
		return nil, upstreamError("build repository file request", err)
	}

	c.auth(req)

	resp, err := c.api.Do(req)
	if err != nil {
		return nil, upstreamError("request repository file "+objectPath, err)
	}

	return resp, nil
}

// OpenRead returns a reader for a repository file starting at pos. The
// authenticated API request is answered either with the body directly (inline
// files) or with a redirect to storage, which is followed without the
// credential.
func (c *Client) OpenRead(ctx context.Context, objectPath string, pos int64) (io.ReadCloser, error) {
	resp, err := c.startRead(ctx, objectPath)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		// The body is served directly. Inline documents are small, so skipping
		// the leading bytes is bounded and cheaper than a second request.
		if pos > 0 {
			if _, err := io.CopyN(io.Discard, resp.Body, pos); err != nil {
				resp.Body.Close()
				return nil, upstreamError(fmt.Sprintf("skip %d bytes of %s", pos, objectPath), err)
			}
		}
		return resp.Body, nil
	}

	if resp.StatusCode < 300 || resp.StatusCode >= 400 {
		defer resp.Body.Close()
		return nil, upstreamStatus("read object "+objectPath, resp)
	}

	location, err := resp.Location()
	resp.Body.Close()
	if err != nil {
		return nil, upstreamError("read object "+objectPath, err)
	}
	if !safeStorageURL(location.String()) {
		return nil, upstreamError("read object "+objectPath,
			fmt.Errorf("upstream redirected to an untrusted host %q", location.Redacted()))
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, location.String(), nil)
	if err != nil {
		return nil, upstreamError("build storage read request", err)
	}

	if pos > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", pos))
	}

	stored, err := c.storage.Do(req)
	if err != nil {
		return nil, upstreamError("read object "+objectPath+" from storage", err)
	}

	if stored.StatusCode != http.StatusOK && stored.StatusCode != http.StatusPartialContent {
		defer stored.Body.Close()
		return nil, upstreamStatus("read object "+objectPath+" from storage", stored)
	}

	return stored.Body, nil
}

// DownloadTarget performs only the authenticated API request and reports the
// signed URL a browser may fetch itself. Inline files are reported with
// (false, nil) because they must be relayed instead.
func (c *Client) DownloadTarget(ctx context.Context, objectPath, hash, name string) (string, bool, error) {
	resp, err := c.startRead(ctx, objectPath)
	if err != nil {
		return "", false, err
	}

	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return "", false, nil
	}

	if resp.StatusCode < 300 || resp.StatusCode >= 400 {
		// The body is read here to explain the failure; the caller's deferred
		// close then releases it.
		return "", false, upstreamStatus("resolve download target", resp)
	}

	location, err := resp.Location()
	if err != nil {
		return "", false, upstreamError("resolve download target", err)
	}
	if !safeStorageURL(location.String()) {
		return "", false, upstreamError("resolve download target",
			fmt.Errorf("upstream redirected to an untrusted host %q", location.Redacted()))
	}
	if !hashRegexp.MatchString(hash) {
		return "", false, upstreamError("resolve download target",
			errors.New("object digest is not a SHA-256"))
	}

	u, err := url.Parse(location.String())
	if err != nil {
		return "", false, upstreamError("resolve download target", err)
	}

	// Upstream splits the digest across path segments and prefixes a bucket
	// path, so compare against the slash-stripped path.
	if !strings.Contains(strings.ReplaceAll(u.Path, "/", ""), hash) {
		return "", false, upstreamError("resolve download target",
			errors.New("upstream redirect does not address the requested object"))
	}

	query := u.Query()
	for _, key := range []string{"filename", "namespace", "repository", "revision", "tag"} {
		query.Del(key)
	}
	u.RawQuery = query.Encode()
	if u.RawQuery != "" {
		u.RawQuery += "&"
	}

	// filename controls the attachment header on the CDN response and is not
	// covered by the signature, so only values that cannot break out of the
	// quoted header are forwarded; otherwise the hash is used.
	u.RawQuery += "filename=" + strings.ReplaceAll(url.QueryEscape(safeFilename(name, hash)), "+", "%20")

	return u.String(), true, nil
}

// safeFilename rejects values that could inject into the upstream
// Content-Disposition header or leak path structure.
func safeFilename(name, hash string) string {
	if name == "" || len(name) > 255 || strings.ContainsAny(name, "\r\n\x00/\\\"") {
		return hash
	}

	return name
}

// Delete removes the given repository objects. Paths already absent from the
// revision are skipped, because ModelScope rejects a commit whose actions
// reference a path that does not exist. When every path is absent the call
// reports success without mutating anything.
func (c *Client) Delete(ctx context.Context, objectPaths ...string) error {
	var actions []map[string]any
	seen := make(map[string]struct{}, len(objectPaths))

	for _, objectPath := range objectPaths {
		if objectPath == "" {
			continue
		}

		if _, ok := seen[objectPath]; ok {
			continue
		}
		seen[objectPath] = struct{}{}

		exists, err := c.Exists(ctx, objectPath)
		if err != nil {
			return err
		}

		if exists {
			actions = append(actions, map[string]any{"action": "delete", "path": objectPath})
		}
	}

	return c.Commit(ctx, actions)
}

// ObjectPath maps a SHA-256 digest to its physical path inside the repository:
// a two-digit namespace followed by the digest split into two path segments.
func ObjectPath(namespace, hash string) string {
	if len(hash) < 2 {
		return path.Join(namespace, hash)
	}

	return path.Join(namespace, hash[:2], hash[2:])
}

// HashFromObjectPath recovers the digest from a physical object path.
func HashFromObjectPath(objectPath string) string {
	parts := strings.Split(strings.Trim(objectPath, "/"), "/")
	if len(parts) < 3 {
		return ""
	}

	hash := parts[len(parts)-2] + parts[len(parts)-1]
	if !hashRegexp.MatchString(hash) {
		return ""
	}

	return hash
}
