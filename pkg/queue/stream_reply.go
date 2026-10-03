package queue

import (
	"fmt"
	"strconv"
)

// parseXRead converts an XREADGROUP reply into entries.
//
// The reply is an array of stream sections, each holding a list of entries, and
// each entry an id followed by a flat field/value list:
//
//	[[stream, [[entryID, [field, value, ...]], ...]], ...]
//
// A nil reply means the stream held nothing new within the block timeout, which
// is reported as an empty result rather than an error.
func parseXRead(reply any) ([]streamEntry, error) {
	if reply == nil {
		return nil, nil
	}

	streams, ok := reply.([]any)
	if !ok || len(streams) == 0 {
		return nil, fmt.Errorf("unexpected XREADGROUP reply of type %T", reply)
	}

	var entries []streamEntry
	for _, stream := range streams {
		section, ok := stream.([]any)
		if !ok || len(section) < 2 {
			return nil, fmt.Errorf("unexpected XREADGROUP stream section of type %T", stream)
		}

		rawEntries, ok := section[1].([]any)
		if !ok {
			return nil, fmt.Errorf("unexpected XREADGROUP entry list of type %T", section[1])
		}

		for _, rawEntry := range rawEntries {
			entry, err := parseStreamEntry(rawEntry)
			if err != nil {
				return nil, err
			}
			entries = append(entries, entry)
		}
	}

	return entries, nil
}

// parseXAutoClaim converts an XAUTOCLAIM reply into entries.
//
// The reply is [next-cursor, entries, [deleted-ids...]]. The trailing element was
// added in Redis 7.0, so it is not required here.
func parseXAutoClaim(reply any) []streamEntry {
	result, ok := reply.([]any)
	if !ok || len(result) < 2 {
		return nil
	}

	rawEntries, ok := result[1].([]any)
	if !ok {
		return nil
	}

	entries := make([]streamEntry, 0, len(rawEntries))
	for _, rawEntry := range rawEntries {
		entry, err := parseStreamEntry(rawEntry)
		if err != nil {
			continue
		}
		entries = append(entries, entry)
	}

	return entries
}

// parseStreamEntry reads one [entryID, [field, value, ...]] pair.
func parseStreamEntry(raw any) (streamEntry, error) {
	pair, ok := raw.([]any)
	if !ok || len(pair) < 2 {
		return streamEntry{}, fmt.Errorf("unexpected stream entry of type %T", raw)
	}

	entry := streamEntry{
		entryID: string(redisBytes(pair[0])),
	}

	fields, ok := pair[1].([]any)
	if !ok {
		return streamEntry{}, fmt.Errorf("unexpected stream entry fields of type %T", pair[1])
	}

	for i := 0; i+1 < len(fields); i += 2 {
		switch string(redisBytes(fields[i])) {
		case "id":
			taskID, err := strconv.Atoi(string(redisBytes(fields[i+1])))
			if err != nil {
				return streamEntry{}, fmt.Errorf("invalid task id %q: %w", redisBytes(fields[i+1]), err)
			}
			entry.taskID = taskID
		case "type":
			entry.taskType = string(redisBytes(fields[i+1]))
		case "recover":
			entry.recover = string(redisBytes(fields[i+1])) == "1"
		}
	}

	if entry.taskID == 0 {
		return streamEntry{}, fmt.Errorf("stream entry %q carries no task id", entry.entryID)
	}

	return entry, nil
}

func redisBytes(value any) []byte {
	switch typed := value.(type) {
	case []byte:
		return typed
	case string:
		return []byte(typed)
	default:
		return []byte(fmt.Sprint(value))
	}
}
