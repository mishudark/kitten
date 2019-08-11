package upperdb

import (
	"sync"

	db "upper.io/db.v3"
	"upper.io/db.v3/lib/sqlbuilder"
)

// DbCollection is used as a wrapper to ensure the db is ready
// nolint: deadcode,unused
type DbCollection func() db.Collection

var cache sync.Map

// Ensure is a closure around ensureCollection
func Ensure(sess sqlbuilder.Database, name string) DbCollection {
	return func() db.Collection {
		return EnsureCollection(sess, name)
	}
}

// EnsureCollection checks in cache if the given name exists, if not it will
// ensure if the collection is available, otherwise it will clear session cache,
// and will try to connect with collection again
func EnsureCollection(sess sqlbuilder.Database, name string) db.Collection { // nolint: deadcode,unused
	if collection, ok := cache.Load(name); ok {
		return collection.(db.Collection)
	}

	// if the collection exists, update cache
	collection := sess.Collection(name)
	if collection.Exists() {
		cache.Store(name, collection)
		return collection
	}

	// we asume that the collection does not exist
	sess.ClearCache()
	return collection
}

// ClearCache should be used only for tests
func ClearCache() {
	cache.Range(func(key, val interface{}) bool {
		cache.Delete(key)
		return true
	})
}
