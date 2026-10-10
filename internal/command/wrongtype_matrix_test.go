package command

import "testing"

// TestWrongTypeMatrix runs every command family against keys of every other
// type and expects WRONGTYPE each time, without any change to those keys. "KEY"
// stands for the key under test; "other" is a set that contains "m". Later
// parts add the sorted-set family here.
func TestWrongTypeMatrix(t *testing.T) {
	families := map[string][][]string{
		"string": {
			{"GET", "KEY"}, {"STRLEN", "KEY"}, {"APPEND", "KEY", "x"}, {"INCR", "KEY"}, {"DECR", "KEY"},
			{"INCRBY", "KEY", "2"}, {"DECRBY", "KEY", "2"}, {"GETDEL", "KEY"}, {"GETEX", "KEY"},
			{"GETEX", "KEY", "EX", "10"}, {"SET", "KEY", "v", "GET"},
		},
		"list": {
			{"LPUSH", "KEY", "a"}, {"RPUSH", "KEY", "a"}, {"LPUSHX", "KEY", "a"}, {"RPUSHX", "KEY", "a"},
			{"LPOP", "KEY"}, {"RPOP", "KEY"}, {"LPOP", "KEY", "2"}, {"LLEN", "KEY"}, {"LRANGE", "KEY", "0", "-1"},
			{"LINDEX", "KEY", "0"}, {"LSET", "KEY", "0", "a"}, {"LREM", "KEY", "0", "a"}, {"LTRIM", "KEY", "0", "1"},
			{"LINSERT", "KEY", "BEFORE", "a", "b"},
		},
		"hash": {
			{"HSET", "KEY", "f", "v"}, {"HSETNX", "KEY", "f", "v"}, {"HGET", "KEY", "f"}, {"HMGET", "KEY", "f"},
			{"HDEL", "KEY", "f"}, {"HGETALL", "KEY"}, {"HKEYS", "KEY"}, {"HVALS", "KEY"}, {"HEXISTS", "KEY", "f"},
			{"HLEN", "KEY"}, {"HSTRLEN", "KEY", "f"}, {"HINCRBY", "KEY", "f", "1"}, {"HINCRBYFLOAT", "KEY", "f", "1"},
		},
		"set": {
			{"SADD", "KEY", "m"}, {"SREM", "KEY", "m"}, {"SMEMBERS", "KEY"}, {"SISMEMBER", "KEY", "m"},
			{"SMISMEMBER", "KEY", "m"}, {"SCARD", "KEY"}, {"SPOP", "KEY"}, {"SPOP", "KEY", "2"},
			{"SRANDMEMBER", "KEY"}, {"SRANDMEMBER", "KEY", "2"},
			{"SMOVE", "KEY", "other", "m"}, {"SMOVE", "other", "KEY", "m"},
			{"SUNION", "KEY", "other"}, {"SUNION", "other", "KEY"},
			{"SINTER", "KEY", "other"}, {"SINTER", "other", "KEY"},
			{"SDIFF", "KEY", "other"}, {"SDIFF", "other", "KEY"},
			{"SUNIONSTORE", "dst", "KEY", "other"}, {"SINTERSTORE", "dst", "other", "KEY"},
			{"SDIFFSTORE", "dst", "KEY", "other"},
		},
	}
	keys := map[string]string{"string": "k_string", "list": "k_list", "hash": "k_hash", "set": "k_set"}

	h := newHarness(t)
	h.run(t, []step{
		do(rOK, "SET", "k_string", "v"),
		do(rInt(1), "RPUSH", "k_list", "a"),
		do(rInt(1), "HSET", "k_hash", "f", "v"),
		do(rInt(1), "SADD", "k_set", "m"),
		do(rInt(1), "SADD", "other", "m"),
	})
	for family, cmds := range families {
		for typ, key := range keys {
			if typ == family {
				continue
			}
			for _, cmd := range cmds {
				argv := make([]string, len(cmd))
				for i, a := range cmd {
					argv[i] = a
					if a == "KEY" {
						argv[i] = key
					}
				}
				if got := h.do(argv...); got != rWrongType {
					t.Errorf("%s command %v against a %s key: got %q, want WRONGTYPE", family, argv, typ, got)
				}
			}
		}
	}

	// None of the rejected commands changed anything.
	h.run(t, []step{
		do(rBulk("v"), "GET", "k_string"),
		do(rInt(1), "LLEN", "k_list"),
		do(rInt(1), "HLEN", "k_hash"),
		do(rInt(1), "SCARD", "k_set"),
		do(rInt(1), "SCARD", "other"),
		do(rInt(0), "EXISTS", "dst"),
	})
}
