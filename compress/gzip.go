/* Copyright 2022 Zinc Labs Inc. and Contributors
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
 */

package compress

import (
	"bytes"
	"compress/gzip"
)

// GzipDecompress decompresses a block using gzip algorithm.
func GzipDecompress(dst, src []byte) ([]byte, error) {
	buf := bytes.NewBuffer(src)
	r, err := gzip.NewReader(buf)
	if err != nil {
		return nil, err
	}
	out := bytes.NewBuffer(dst[:0])
	out.ReadFrom(r)
	return out.Bytes(), nil
}

// GzipCompress compresses a block using gzip algorithm.
func GzipCompress(dst, src []byte) ([]byte, error) {
	buf := bytes.NewBuffer(dst[:0])
	w := gzip.NewWriter(buf)
	_, err := w.Write(src)
	if err != nil {
		return nil, err
	}
	if err = w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
