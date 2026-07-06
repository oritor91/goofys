// Copyright 2015 - 2017 Ka-Hing Cheung
// Copyright 2015 - 2017 Google Inc. All Rights Reserved.
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

// System permissions-related code.
package internal

import (
	"os"
)

// Return the UID and GID of this process.
//
// This intentionally avoids os/user.Current(), which requires either cgo or
// a matching /etc/passwd entry (or $USER/$HOME set) to resolve the current
// UID/GID to a User struct. That fails inside minimal/statically-linked
// containers running as an arbitrary UID with no passwd entry, e.g. CI
// runners. os.Getuid/Getgid read the process's real IDs directly and need
// no lookup.
func MyUserAndGroup() (uid int, gid int) {
	return os.Getuid(), os.Getgid()
}
