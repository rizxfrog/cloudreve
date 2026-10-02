package modelscope

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"regexp"
	"time"

	"github.com/cloudreve/Cloudreve/v4/ent"
	"github.com/cloudreve/Cloudreve/v4/inventory/types"
	"github.com/cloudreve/Cloudreve/v4/pkg/boolset"
	"github.com/cloudreve/Cloudreve/v4/pkg/conf"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/driver"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/fs"
	"github.com/cloudreve/Cloudreve/v4/pkg/logging"
	"github.com/cloudreve/Cloudreve/v4/pkg/setting"
)

const (
	defaultEndpoint  = "https://www.modelscope.cn"
	defaultRepoType  = "datasets"
	defaultRevision  = "master"
	defaultNamespace = "00"
	namespacePattern = `^[0-9]{2}$`
)

var (
	features = &boolset.BooleanSet{}

	// namespaceRegexp validates the two-digit physical path namespace.
	namespaceRegexp = regexp.MustCompile(namespacePattern)
)

func init() {
	boolset.Sets(map[driver.HandlerCapability]bool{
		// Uploads must always relay: the object path derives from the SHA-256 of
		// the content, so no upload target can be named before the stream has
		// been read, and the repository token never leaves this server.
		driver.HandlerCapabilityUploadProxyRequired: true,
		// Objects are addressed by the SHA-256 of their content, so two
		// entities can share one physical object.
		driver.HandlerCapabilityContentAddressed: true,
		// The object path derives from the content digest, which is only known
		// after the stream has been read.
		driver.HandlerCapabilitySourceDeferred: true,
	}, features)
}

// Driver stores file content in a ModelScope repository. Only the bytes live
// upstream; the directory tree and file metadata stay in Cloudreve's database.
//
// The physical object path is derived from the SHA-256 of the content:
// "<namespace>/<hash[:2]>/<hash[2:]>". Because the digest is only known once
// the stream has been read, the resolved path is written back into the upload
// request and persisted onto the entity when the upload completes.
type Driver struct {
	policy *ent.StoragePolicy
	client *Client
	ns     string
	l      logging.Logger
}

// New constructs a ModelScope storage driver.
func New(ctx context.Context, policy *ent.StoragePolicy, settings setting.Provider,
	config conf.ConfigProvider, l logging.Logger) (*Driver, error) {
	policySettings := policy.Settings
	if policySettings == nil {
		policySettings = &types.PolicySetting{}
	}

	endpoint := policy.Server
	if endpoint == "" {
		endpoint = defaultEndpoint
	}

	repoType := policySettings.ModelScopeRepoType
	if repoType == "" {
		repoType = defaultRepoType
	}

	revision := policySettings.ModelScopeRevision
	if revision == "" {
		revision = defaultRevision
	}

	ns := policySettings.ModelScopeNamespace
	if ns == "" {
		ns = defaultNamespace
	}

	if !namespaceRegexp.MatchString(ns) {
		return nil, errors.New("ModelScope namespace must be exactly two digits")
	}

	client, err := NewClient(endpoint, policy.SecretKey, policy.BucketName, repoType, revision, l)
	if err != nil {
		return nil, err
	}

	return &Driver{
		policy: policy,
		client: client,
		ns:     ns,
		l:      l,
	}, nil
}

// ErrDigestMismatch is returned when the content received does not hash to the
// digest the client declared. The declared value decides the physical object
// path, and content-addressed objects are shared between entities, so accepting
// a mismatch would file content under a path that describes other bytes.
var ErrDigestMismatch = errors.New("content does not match the declared hash")

// Put stores the file content in ModelScope and rewrites the request's save
// path to the resolved content-addressed object path.
//
// The object path derives from the SHA-256 of the content, so the digest is
// needed before the content can be addressed. When the client supplies it, the
// object is addressed up front and the content is streamed straight to storage
// without being buffered. Without it the only way to learn the digest is to
// read the whole stream, which falls back to a temporary file.
func (d *Driver) Put(ctx context.Context, file *fs.UploadRequest) error {
	defer file.Close()

	// The object path is derived from the whole content, so the request must
	// carry the entire file in one piece. Uploads always relay through the
	// server, which guarantees a single chunk starting at offset zero.
	if file.Offset != 0 {
		return errors.New("ModelScope storage only supports whole-file uploads")
	}

	if declared := file.Props.ClientHash; declared != "" {
		return d.putStreamed(ctx, file, declared)
	}

	return d.putBuffered(ctx, file)
}

// putBuffered reads the whole stream into a temporary file to learn the digest,
// then stores it. It is the fallback for a client that cannot hash the content
// before sending it.
func (d *Driver) putBuffered(ctx context.Context, file *fs.UploadRequest) error {
	temp, err := os.CreateTemp("", "cloudreve-modelscope-*")
	if err != nil {
		return fmt.Errorf("failed to create temporary file: %w", err)
	}

	defer func() {
		_ = temp.Close()
		_ = os.Remove(temp.Name())
	}()

	hasher := sha256.New()
	written, err := io.Copy(io.MultiWriter(temp, hasher), io.LimitReader(file, file.Props.Size+1))
	if err != nil {
		return fmt.Errorf("failed to buffer upload: %w", err)
	}

	if written != file.Props.Size {
		return errors.New("uploaded data does not match the declared size")
	}

	hash := hex.EncodeToString(hasher.Sum(nil))
	objectPath := ObjectPath(d.ns, hash)

	if err := d.store(ctx, temp, objectPath, hash, file.Props.Size); err != nil {
		return err
	}

	// Record the resolved path so the manager can persist it onto the entity.
	file.Props.SavePath = objectPath
	return nil
}

// putStreamed stores the content described by declared without buffering it.
//
// The bytes are relayed to storage through a pipe that withholds the stream
// until the digest has been verified, so a client cannot get content filed
// under a path it does not hash to. The upstream PUT only receives data while
// the pipe is open; a mismatch closes it with an error before any byte is
// accepted, and the storage target is never committed.
func (d *Driver) putStreamed(ctx context.Context, file *fs.UploadRequest, declared string) error {
	size := file.Props.Size
	objectPath := ObjectPath(d.ns, declared)

	if size <= inlineLimit {
		return d.putInlineStreamed(ctx, file, declared, objectPath, size)
	}

	// The probe is a deduplication shortcut whose failure must not abort an
	// otherwise valid upload.
	exists := false
	if probed, err := d.client.Exists(ctx, objectPath); err != nil {
		d.l.Warning("ModelScope object probe for %q failed, proceeding with upload: %s", objectPath, err)
	} else {
		exists = probed
	}

	if exists {
		// Identical content already resolves to this object. The body is still
		// read so the digest is verified before the caller's upload succeeds.
		if err := drainAndVerify(file, size, declared); err != nil {
			return err
		}
		return d.client.Commit(ctx, []map[string]any{blobAction(objectPath, declared, size)})
	}

	target, err := d.client.Validate(ctx, declared, size)
	if err != nil {
		return err
	}

	if target == "" {
		// The blob is already stored upstream and only the pointer is missing.
		if err := drainAndVerify(file, size, declared); err != nil {
			return err
		}
		return d.client.Commit(ctx, []map[string]any{blobAction(objectPath, declared, size)})
	}

	// Relay the body to storage while hashing it. The bytes cannot be withheld
	// until the digest is known, because that would mean buffering the whole
	// upload, which is what this path exists to avoid. The guarantee comes from
	// two places instead:
	//
	//   1. Storage itself rejects a blob whose digest does not match the oid it
	//      was addressed with, so mismatched content is never stored.
	//   2. The pointer is only committed after the digest is verified locally,
	//      so no entity can ever reference an object it does not describe.
	//
	// A mismatch therefore aborts the transfer and skips the commit; nothing is
	// left pointing at the wrong bytes.
	pr, pw := io.Pipe()
	hasher := sha256.New()
	counted := &countingReader{r: io.TeeReader(io.LimitReader(file, size+1), hasher)}
	relayed := make(chan error, 1)

	go func() {
		_, copyErr := io.Copy(pw, counted)

		if copyErr == nil {
			copyErr = verifyStreamed(counted, hasher, size, declared)
		}

		// Closing with the error aborts an in-flight PUT and makes storage
		// discard whatever it received.
		pw.CloseWithError(copyErr)
		relayed <- copyErr
	}()

	putErr := d.client.Put(ctx, target, declared, pr, size)

	// Put can return before the body is fully consumed, for example when
	// storage answers an error early or the request fails to start. Closing the
	// reader unblocks the relay goroutine, which would otherwise sit in a pipe
	// write with no reader and never reach the channel below.
	_ = pr.Close()

	// The relay goroutine always reports its verdict, so this cannot block.
	relayErr := <-relayed

	if putErr != nil {
		return putErr
	}
	if relayErr != nil {
		return relayErr
	}

	if err := d.client.Commit(ctx, []map[string]any{blobAction(objectPath, declared, size)}); err != nil {
		return err
	}

	file.Props.SavePath = objectPath
	return nil
}

// putInlineStreamed handles an object small enough to be committed inline. Its
// content travels inside the commit request, so the bytes are held in a bounded
// buffer and the digest is checked before anything is sent.
func (d *Driver) putInlineStreamed(ctx context.Context, file *fs.UploadRequest, declared, objectPath string, size int64) error {
	hasher := sha256.New()
	counted := &countingReader{r: io.TeeReader(io.LimitReader(file, size+1), hasher)}

	content, err := io.ReadAll(counted)
	if err != nil {
		return fmt.Errorf("failed to read upload: %w", err)
	}

	if err := verifyStreamed(counted, hasher, size, declared); err != nil {
		return err
	}

	if err := d.client.Commit(ctx, []map[string]any{inlineAction(objectPath, content)}); err != nil {
		return err
	}

	file.Props.SavePath = objectPath
	return nil
}

// drainAndVerify consumes the body and checks it against the declared digest.
// Used on the paths that do not relay the bytes to storage, so the caller still
// has to prove the content matches what it claimed.
func drainAndVerify(file *fs.UploadRequest, size int64, declared string) error {
	hasher := sha256.New()
	counted := &countingReader{r: io.TeeReader(io.LimitReader(file, size+1), hasher)}

	if _, err := io.Copy(io.Discard, counted); err != nil {
		return fmt.Errorf("failed to read upload: %w", err)
	}

	return verifyStreamed(counted, hasher, size, declared)
}

// countingReader counts the bytes that passed through it.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// verifyStreamed rejects a stream whose length or digest does not match what
// the client declared. A short or long stream is caught first so a truncated
// upload is never committed.
func verifyStreamed(counted *countingReader, hasher hash.Hash, size int64, declared string) error {
	if counted.n != size {
		return upstreamError("upload content", fmt.Errorf("declared %d bytes, received %d", size, counted.n))
	}

	if actual := hex.EncodeToString(hasher.Sum(nil)); actual != declared {
		return fmt.Errorf("%w: declared %s, computed %s", ErrDigestMismatch, declared, actual)
	}

	return nil
}

// store commits the object at objectPath, uploading the blob first when it is
// not already present. An existing object is left untouched: identical content
// maps to the same path, so a repeated upload is a no-op.
func (d *Driver) store(ctx context.Context, temp *os.File, objectPath, hash string, size int64) error {
	// The probe is only a deduplication shortcut: Validate and Commit already
	// report an already-stored object correctly. A failed probe must not abort
	// the upload, or a network that answers reads but refuses writes (or the
	// reverse) turns a working upload into a failure.
	exists, err := d.client.Exists(ctx, objectPath)
	if err != nil {
		d.l.Warning("ModelScope object probe for %q failed, proceeding with upload: %s", objectPath, err)
		exists = false
	}

	if exists {
		// Identical content already resolves to this object, so the upload is a
		// no-op and the existing blob stays referenced.
		d.l.Debug("ModelScope object %q already exists, reusing stored content", objectPath)
		return nil
	}

	if size <= inlineLimit {
		if _, err := temp.Seek(0, io.SeekStart); err != nil {
			return fmt.Errorf("failed to rewind temporary file: %w", err)
		}

		content, err := io.ReadAll(temp)
		if err != nil {
			return fmt.Errorf("failed to read temporary file: %w", err)
		}

		return d.client.Commit(ctx, []map[string]any{inlineAction(objectPath, content)})
	}

	target, err := d.client.Validate(ctx, hash, size)
	if err != nil {
		return err
	}

	// An empty target means the blob already exists upstream and only the
	// pointer needs to be committed.
	if target != "" {
		if _, err := temp.Seek(0, io.SeekStart); err != nil {
			return fmt.Errorf("failed to rewind temporary file: %w", err)
		}

		if err := d.client.Put(ctx, target, hash, temp, size); err != nil {
			return err
		}
	}

	return d.client.Commit(ctx, []map[string]any{blobAction(objectPath, hash, size)})
}

// Delete removes the given physical objects.
func (d *Driver) Delete(ctx context.Context, files ...string) ([]string, error) {
	d.l.Debug("Deleting %d object(s) from ModelScope repository", len(files))

	if err := d.client.Delete(ctx, files...); err != nil {
		return files, err
	}

	return nil, nil
}

// Open is only available for handlers that can read a local path.
func (d *Driver) Open(ctx context.Context, path string) (*os.File, error) {
	return nil, errors.New("not implemented")
}

// LocalPath is only available for handlers that store data locally.
func (d *Driver) LocalPath(ctx context.Context, path string) string {
	return ""
}

// Thumb returns the URL for a thumbnail of the given entity. Thumbnails are
// generated by Cloudreve's local pipeline, so no native URL is offered; the
// sentinel asks the caller to serve the generated object through this server.
func (d *Driver) Thumb(ctx context.Context, expire *time.Time, ext string, e fs.Entity) (string, error) {
	return "", driver.ErrNoPublicUrl
}

// Source returns a browser fetchable URL for the entity, or ErrNoPublicUrl when
// the object has none. Downloads default to the relay, so this is only reached
// for a policy that opted out of it.
func (d *Driver) Source(ctx context.Context, e fs.Entity, args *driver.GetSourceArgs) (string, error) {
	url, signed, err := d.client.DownloadTarget(ctx, e.Source(), HashFromObjectPath(e.Source()), args.DisplayName)
	if err != nil {
		return "", err
	}

	if !signed {
		return "", driver.ErrNoPublicUrl
	}

	return url, nil
}

// HasPublicSource reports whether the object is stored as a fetchable blob.
// Objects at or below the inline limit live inside the repository and have no
// URL of their own, so they can only be read through this server.
func (d *Driver) HasPublicSource(e fs.Entity) bool {
	return e.Size() > inlineLimit
}

// OpenStream returns a reader for the object content, supporting both LFS
// blobs (served via a redirect) and inline repository files.
func (d *Driver) OpenStream(ctx context.Context, e fs.Entity, pos int64) (io.ReadCloser, error) {
	return d.client.OpenRead(ctx, e.Source(), pos)
}

// Token is not supported because objects are content addressed: the upload
// target cannot be known before the content is read. Uploads must relay
// through Cloudreve, which is enforced by the policy settings.
func (d *Driver) Token(ctx context.Context, uploadSession *fs.UploadSession, file *fs.UploadRequest) (*fs.UploadCredential, error) {
	return nil, errors.New("ModelScope storage requires relayed uploads")
}

// CancelToken is a no-op: relayed uploads hold no upstream session.
func (d *Driver) CancelToken(ctx context.Context, uploadSession *fs.UploadSession) error {
	return nil
}

// CompleteUpload is a no-op: the object is committed as part of Put.
func (d *Driver) CompleteUpload(ctx context.Context, session *fs.UploadSession) error {
	return nil
}

// List is unsupported: the repository holds content addressed blobs and no
// user visible directory structure.
func (d *Driver) List(ctx context.Context, base string, onProgress driver.ListProgressFunc, recursive bool) ([]fs.PhysicalObject, error) {
	return nil, errors.New("not implemented")
}

// MediaMeta is unsupported; metadata is extracted by Cloudreve's local
// generators.
func (d *Driver) MediaMeta(ctx context.Context, path, ext, language string) ([]driver.MediaMeta, error) {
	return nil, errors.New("not implemented")
}

// Capabilities describes the features of this handler.
func (d *Driver) Capabilities() *driver.Capabilities {
	return &driver.Capabilities{
		StaticFeatures: features,
		// Large objects exceed the server's tolerance for whole-object
		// retrieval, so metadata is extracted through the internal proxy.
		MediaMetaProxy: true,
		ThumbProxy:     true,
	}
}
