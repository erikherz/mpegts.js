/*
 * Copyright (C) 2025 Vivoh, Inc. All Rights Reserved.
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

import Log from '../utils/logger.js';
import {BaseLoader, LoaderStatus, LoaderErrors} from './loader.js';
import {RuntimeException} from '../utils/exception.js';

class WebTransportLoader extends BaseLoader {
    constructor() {
        super('webtransport-loader');
        this.TAG = 'WebTransportLoader';
        this._needStash = true;
        this._transport = null;
        this._reader = null;
        this._requestAbort = false;
        this._receivedLength = 0;

        // For TS packet handling
        this._buffer = new Uint8Array(0);
        this.PACKET_SIZE = 188;
        this.SYNC_BYTE = 0x47;
    }

    static isSupported() {
        try {
            return (typeof self.WebTransport !== 'undefined');
        } catch (e) {
            return false;
        }
    }

    destroy() {
        if (this._transport) {
            this.abort();
        }
        super.destroy();
    }

    async open(dataSource) {
        try {
            if (!dataSource.url.startsWith('https://')) {
                throw new Error('WebTransport requires HTTPS URL');
            }

            this._transport = new WebTransport(dataSource.url);
            await this._transport.ready;

            const incomingStreams = this._transport.incomingUnidirectionalStreams;
            const streamReader = incomingStreams.getReader();
            const {value: stream} = await streamReader.read();
            if (!stream) {
                throw new Error('No incoming stream received');
            }

            this._reader = stream.getReader();
            this._status = LoaderStatus.kBuffering;

            this._readChunks();

        } catch (e) {
            this._status = LoaderStatus.kError;
            if (this._onError) {
                this._onError(LoaderErrors.EXCEPTION, {code: e.code || -1, msg: e.message});
            }
        }
    }

    abort() {
        if (this._transport && !this._transport.closed) {
            this._requestAbort = true;
            if (this._reader) {
                this._reader.cancel();
            }
            this._transport.close();
        }
        this._transport = null;
        this._reader = null;
        this._status = LoaderStatus.kComplete;
    }

    // Core packet processing methods
    _processChunk(chunk) {
        // Merge with existing buffer
        const newBuffer = new Uint8Array(this._buffer.length + chunk.length);
        newBuffer.set(this._buffer);
        newBuffer.set(chunk, this._buffer.length);
        this._buffer = newBuffer;

        // Find sync byte
        let syncIndex = -1;
        for (let i = 0; i < this._buffer.length; i++) {
            if (this._buffer[i] === this.SYNC_BYTE) {
                syncIndex = i;
                break;
            }
        }

        if (syncIndex === -1) return null;

        // Remove data before sync if necessary
        if (syncIndex > 0) {
            this._buffer = this._buffer.slice(syncIndex);
        }

        // Extract complete packets
        const completePackets = Math.floor(this._buffer.length / this.PACKET_SIZE);
        if (completePackets === 0) return null;

        const packetsData = this._buffer.slice(0, completePackets * this.PACKET_SIZE);
        this._buffer = this._buffer.slice(completePackets * this.PACKET_SIZE);

        return this._validatePackets(packetsData);
    }

    _validatePackets(packetsData) {
        const validPackets = [];
        for (let i = 0; i < packetsData.length; i += this.PACKET_SIZE) {
            const packet = packetsData.slice(i, i + this.PACKET_SIZE);
            if (packet.length === this.PACKET_SIZE && packet[0] === this.SYNC_BYTE) {
                validPackets.push(...packet);
            }
        }
        return validPackets.length > 0 ? new Uint8Array(validPackets) : null;
    }

    async _readChunks() {
        try {
            while (true) {
                const {value, done} = await this._reader.read();
                if (done || this._requestAbort) break;

                if (value) {
                    const chunk = value instanceof Uint8Array ? value : new Uint8Array(value);
                    const packets = this._processChunk(chunk);
                    if (packets) {
                        const byteStart = this._receivedLength;
                        this._receivedLength += packets.byteLength;
                        if (this._onDataArrival) {
                            this._onDataArrival(packets.buffer, byteStart, this._receivedLength);
                        }
                    }
                }
            }

            if (!this._requestAbort) {
                this._status = LoaderStatus.kComplete;
                if (this._onComplete) {
                    this._onComplete(0, this._receivedLength - 1);
                }
            }
        } catch (e) {
            this._status = LoaderStatus.kError;
            if (this._onError) {
                this._onError(LoaderErrors.EXCEPTION, {code: e.code || -1, msg: e.message});
            }
        }
    }
}
export default WebTransportLoader;
