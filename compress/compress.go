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
	"errors"
)

const (
	SNAPPY = iota
	ZSTD
	S2
)

var Algorithm = S2

func Decompress(dst, src []byte) ([]byte, error) {
	switch Algorithm {
	case SNAPPY:
		return SnappyDecompress(dst, src)
	case ZSTD:
		return ZSTDDecompress(dst, src)
	case S2:
		return S2Decompress(dst, src)
	default:
		return nil, errors.New("unknown compress algorithm")
	}
}

func Compress(dst, src []byte) ([]byte, error) {
	switch Algorithm {
	case SNAPPY:
		return SnappyCompress(dst, src)
	case ZSTD:
		return ZSTDCompress(dst, src, ZSTDCompressionLevel)
	case S2:
		return S2Compress(dst, src)
	default:
		return nil, errors.New("unknown compress algorithm")
	}
}
