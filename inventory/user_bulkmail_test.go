package inventory

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"entgo.io/ent/dialect"
	"entgo.io/ent/dialect/sql"
	"github.com/cloudreve/Cloudreve/v4/ent"
	"github.com/cloudreve/Cloudreve/v4/ent/enttest"
	"github.com/cloudreve/Cloudreve/v4/ent/user"
	"github.com/cloudreve/Cloudreve/v4/inventory/types"
	"github.com/cloudreve/Cloudreve/v4/pkg/boolset"
	"github.com/stretchr/testify/require"
)

// newTestDB opens a temporary SQLite database with the schema applied and one
// group, so seeded users have somewhere to belong.
func newTestDB(t *testing.T) *ent.Client {
	t.Helper()

	client := enttest.Open(t, "sqlite3", filepath.Join(t.TempDir(), "user.db"))
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	_, err := client.Group.Create().
		SetName("test").
		SetPermissions(&boolset.BooleanSet{}).
		SetMaxStorage(0).
		SetSpeedLimit(0).
		SetSettings(&types.GroupSetting{}).
		Save(context.Background())
	require.NoError(t, err)

	return client
}

func seedUser(t *testing.T, client *ent.Client, email, nick string, status user.Status) *ent.User {
	t.Helper()

	u, err := client.User.Create().
		SetEmail(email).
		SetNick(nick).
		SetStatus(status).
		SetGroupID(1).
		Save(context.Background())
	require.NoError(t, err)

	return u
}

// TestCursorWalkVisitsEveryRecipientExactlyOnce is the property a bulk send
// depends on: paging by ascending ID must cover the whole matching set without
// repeating anyone, including across batch boundaries.
func TestCursorWalkVisitsEveryRecipientExactlyOnce(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	client := NewUserClient(db)

	const total = 40
	const batch = 7

	var expected []int
	for i := range total {
		u := seedUser(t, db, fmt.Sprintf("user%d@example.com", i), "nick", user.StatusActive)
		expected = append(expected, u.ID)
	}

	var visited []int
	cursor := 0
	for {
		users, err := client.ListUsersAfterID(ctx, &ListUserAfterIDParameters{AfterID: cursor, Limit: batch})
		require.NoError(t, err)
		if len(users) == 0 {
			break
		}
		for _, u := range users {
			visited = append(visited, u.ID)
		}
		cursor = users[len(users)-1].ID
	}

	require.Equal(t, expected, visited)
}

func TestListUsersAfterIDRespectsFilter(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	client := NewUserClient(db)

	seedUser(t, db, "a@example.com", "alpha", user.StatusActive)
	inactive := seedUser(t, db, "b@example.com", "beta", user.StatusInactive)
	sysBanned := seedUser(t, db, "c@example.com", "gamma", user.StatusSysBanned)

	users, err := client.ListUsersAfterID(ctx, &ListUserAfterIDParameters{
		Limit:      10,
		UserFilter: UserFilter{Statuses: []user.Status{user.StatusInactive, user.StatusSysBanned}},
	})
	require.NoError(t, err)

	var ids []int
	for _, u := range users {
		ids = append(ids, u.ID)
	}
	require.ElementsMatch(t, []int{inactive.ID, sysBanned.ID}, ids)
}

// TestEmailSuffixFilterIsCaseFoldedAndAnchored covers both halves of the suffix
// contract: matching ignores case, and a suffix must end the address rather than
// appear anywhere in it.
func TestEmailSuffixFilterIsCaseFoldedAndAnchored(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	client := NewUserClient(db)

	upper := seedUser(t, db, "First@Example.COM", "one", user.StatusActive)
	lower := seedUser(t, db, "second@example.com", "two", user.StatusActive)
	seedUser(t, db, "third@notexample.com", "three", user.StatusActive)
	seedUser(t, db, "fourth@example.org", "four", user.StatusActive)

	users, err := client.ListUsersAfterID(ctx, &ListUserAfterIDParameters{
		Limit:      10,
		UserFilter: UserFilter{EmailSuffixes: []string{"@example.com"}},
	})
	require.NoError(t, err)

	var ids []int
	for _, u := range users {
		ids = append(ids, u.ID)
	}
	require.ElementsMatch(t, []int{upper.ID, lower.ID}, ids)
}

// TestEmailSuffixFilterTreatsWildcardsAsLiterals guards the LIKE escaping: "_" is
// a single-character wildcard, so a domain containing one must not match an
// unrelated address.
func TestEmailSuffixFilterTreatsWildcardsAsLiterals(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	client := NewUserClient(db)

	literal := seedUser(t, db, "a@my_site.com", "one", user.StatusActive)
	seedUser(t, db, "b@myxsite.com", "two", user.StatusActive)

	users, err := client.ListUsersAfterID(ctx, &ListUserAfterIDParameters{
		Limit:      10,
		UserFilter: UserFilter{EmailSuffixes: []string{"@my_site.com"}},
	})
	require.NoError(t, err)
	require.Len(t, users, 1)
	require.Equal(t, literal.ID, users[0].ID)
}

// TestExcludedSuffixCountsPlaceholders covers the count a bulk mail preview
// reports: the recipients with a real mailbox, and by subtraction the ones that
// will be skipped.
func TestExcludedSuffixCountsPlaceholders(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	client := NewUserClient(db)

	real := seedUser(t, db, "real@example.com", "one", user.StatusActive)
	seedUser(t, db, "placeholder@login.qq.com", "two", user.StatusActive)

	all, err := client.CountUsers(ctx, UserFilter{})
	require.NoError(t, err)
	require.Equal(t, 2, all)

	deliverable, err := client.CountUsers(ctx, UserFilter{NotEmailSuffixes: []string{"@login.qq.com"}})
	require.NoError(t, err)
	require.Equal(t, 1, deliverable)

	users, err := client.ListUsersAfterID(ctx, &ListUserAfterIDParameters{
		Limit:      10,
		UserFilter: UserFilter{NotEmailSuffixes: []string{"@login.qq.com"}},
	})
	require.NoError(t, err)
	require.Len(t, users, 1)
	require.Equal(t, real.ID, users[0].ID)
}

func TestCountUsersMatchesFilteredList(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	client := NewUserClient(db)

	seedUser(t, db, "a@example.com", "match", user.StatusActive)
	seedUser(t, db, "b@example.com", "match", user.StatusActive)
	seedUser(t, db, "c@example.com", "other", user.StatusActive)

	filter := UserFilter{Nick: "match"}
	count, err := client.CountUsers(ctx, filter)
	require.NoError(t, err)

	users, err := client.ListUsersAfterID(ctx, &ListUserAfterIDParameters{Limit: 10, UserFilter: filter})
	require.NoError(t, err)

	require.Equal(t, 2, count)
	require.Equal(t, count, len(users))
}

// TestSuffixPredicateBindsArgumentsPerDialect guards a defect that no SQLite
// test can reach.
//
// The "ends with" predicate is hand-written because ent generates no folding
// suffix variant. Writing it as a literal "?" in the SQL text works on SQLite
// and MySQL, which take "?" placeholders, but PostgreSQL numbers its arguments
// as $n: lib/pq then receives "LIKE ?" with nothing to bind it to and fails the
// whole query, so the bulk mail preview returned a database error while every
// SQLite test passed. The predicate must therefore hand its argument to the
// dialect-aware builder instead of embedding a placeholder in the string.
func TestSuffixPredicateBindsArgumentsPerDialect(t *testing.T) {
	cases := []struct {
		dialect string
		// marker is the placeholder the driver must receive for the argument.
		marker string
	}{
		{dialect.SQLite, "?"},
		{dialect.MySQL, "?"},
		{dialect.Postgres, "$1"},
	}

	for _, c := range cases {
		t.Run(c.dialect, func(t *testing.T) {
			sel := sql.Dialect(c.dialect).
				Select(user.Columns...).
				From(sql.Table(user.Table))
			userHasSuffixFold("@Example.com")(sel)
			query, args := sel.Query()

			require.Contains(t, query, c.marker,
				"query must carry the placeholder %q for its dialect", c.marker)
			// A bare "?" on a dialect that numbers its arguments is the bug.
			if c.marker == "$1" {
				require.NotContains(t, query, "?",
					"no unnumbered placeholder may survive into a numbered dialect")
			}
			require.Contains(t, args, "%@example.com",
				"the suffix must be lower-cased and matched as a trailing pattern: %s", args)
		})
	}
}

// TestSuffixPredicateEscapesWildcards proves a domain containing LIKE
// metacharacters is matched literally, and that only SQLite needs the
// accompanying ESCAPE clause: MySQL and PostgreSQL already default to a
// backslash escape.
func TestSuffixPredicateEscapesWildcards(t *testing.T) {
	build := func(d string) (string, []any) {
		sel := sql.Dialect(d).Select(user.Columns...).From(sql.Table(user.Table))
		userHasSuffixFold("a_b.com")(sel)
		return sel.Query()
	}

	for _, d := range []string{dialect.SQLite, dialect.MySQL, dialect.Postgres} {
		_, args := build(d)
		require.Contains(t, args, "%a\\_b.com",
			"%s: the underscore must be escaped so it cannot act as a wildcard", d)
	}

	sqliteQ, _ := build(dialect.SQLite)
	require.Contains(t, sqliteQ, "ESCAPE")

	mysqlQ, _ := build(dialect.MySQL)
	require.NotContains(t, mysqlQ, "ESCAPE")

	pgQ, _ := build(dialect.Postgres)
	require.NotContains(t, pgQ, "ESCAPE")
}
