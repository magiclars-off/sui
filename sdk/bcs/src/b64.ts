// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

export function fromB64(sBase64: string): Uint8Array {
	return Uint8Array.from(atob(sBase64), (char) => char.charCodeAt(0));
}

export function toB64(aBytes: Uint8Array): string {
	const output = [];
	for (let i = 0; i < aBytes.length; i++) {
		output.push(String.fromCharCode(aBytes[i]));
	}
	return btoa(output.join(''));
}
