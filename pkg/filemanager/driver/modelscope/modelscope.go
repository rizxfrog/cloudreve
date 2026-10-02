package modelscope

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
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

// Put stores the file content in ModelScope and rewrites the request's save
// path to the resolved content-addressed object path.
func (d *Driver) Put(ctx context.Context, file *fs.UploadRequest) error {
	defer file.Close()

	// The object path is derived from the whole content, so the request must
	// carry the entire file in one piece. Uploads always relay through the
	// server, which guarantees a single chunk starting at offset zero.
	if file.Offset != 0 {
		return errors.New("ModelScope storage only supports whole-file uploads")
	}

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
