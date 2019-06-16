// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other Dragonboat authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cpp

import (
	"sync"
	"sync/atomic"
)

type managedObject struct {
	nextObjectID uint64
	objects      sync.Map
}

func newManagedObject() *managedObject {
	return &managedObject{nextObjectID: 1}
}

var bindingObjects = newManagedObject()

// AddManagedObject adds the specified Go object to the managedObject
// collection so they can be access from foreign functions.
func AddManagedObject(object interface{}) uint64 {
	oid := atomic.AddUint64(&bindingObjects.nextObjectID, 1)
	if oid == 0 {
		// takes a loooooong time to overflow
		oid = 1
	}
	bindingObjects.objects.Store(oid, object)
	return oid
}

// GetManagedObject returns the Go object specified by the oid value.
func GetManagedObject(oid uint64) (interface{}, bool) {
	return bindingObjects.objects.Load(oid)
}

// RemoveManagedObject returns the object specified by the oid value from the
// managedObject collection.
func RemoveManagedObject(oid uint64) {
	bindingObjects.objects.Delete(oid)
}

// GetManagedObjectCount returns the number of object in the managed objects
// collection.
func GetManagedObjectCount() uint64 {
	count := uint64(0)
	bindingObjects.objects.Range(func(k, v interface{}) bool {
		count++
		return true
	})
	return count
}
