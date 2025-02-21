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

class PacketLogger {
    constructor(onLog) {
        this.packetCount = 0;
        this.onLog = onLog || (() => {});
    }

    logPacket(packet, timeReceived) {
        this.packetCount++;

        // Only log the 1st, 100th, 1000th, and every 1000th after that
        if (this.packetCount === 1 || this.packetCount === 100 || this.packetCount === 1000 || this.packetCount % 1000 === 0) {
            const pts = this._extractPTS(packet);
            this.onLog(`[PacketLogger] #${this.packetCount} | PTS: ${pts} | Received at: ${timeReceived}`);
        }
    }

    _extractPTS(packet) {
        if (packet.length < 14 || (packet[7] & 0x80) === 0) {
            return "N/A";
        }

        const pts = ((packet[9] & 0x0E) << 29) |
                    ((packet[10] & 0xFF) << 22) |
                    ((packet[11] & 0xFE) << 14) |
                    ((packet[12] & 0xFF) << 7) |
                    ((packet[13] & 0xFE) >> 1);

        return pts;
    }
}

class MPEGTSBuffer {
    constructor(onLog) {
        this.PACKET_SIZE = 188;
        this.SYNC_BYTE = 0x47;
        this.MAX_BUFFER_SIZE = 1024 * 1024; // 1MB
        this.buffer = new Uint8Array(0);
        this.onLog = onLog || (() => {});
    }

	addChunk(chunk) {
	    if (!chunk || chunk.length === 0) return null;

	    const inputChunk = (chunk instanceof Uint8Array) ? chunk : new Uint8Array(chunk);

	    // Append new data to existing buffer
	    let newBuffer = new Uint8Array(this.buffer.length + inputChunk.length);
	    newBuffer.set(this.buffer, 0);
	    newBuffer.set(inputChunk, this.buffer.length);
	    this.buffer = newBuffer;

	    // Find first valid sync byte
	    let syncIndex = this._findSyncByte(this.buffer);
	    if (syncIndex === -1) {
		this.buffer = new Uint8Array(0); // 🛑 Reset buffer if no sync found (avoid corrupt state)
		return null;
	    }

	    // Trim before first valid sync byte
	    if (syncIndex > 0) {
		this.buffer = this.buffer.slice(syncIndex);
	    }

	    // Process only full packets
	    let completePackets = Math.floor(this.buffer.length / this.PACKET_SIZE);
	    if (completePackets === 0) return null;

	    let packetsData = this.buffer.slice(0, completePackets * this.PACKET_SIZE);
	    this.buffer = this.buffer.slice(completePackets * this.PACKET_SIZE);

	    return this._validatePackets(packetsData);
	}

	_findSyncByte(buffer) {
	    for (let i = 0; i <= buffer.length - this.PACKET_SIZE; i++) {
		if (buffer[i] === this.SYNC_BYTE) {
		    // Check if the next MPEG-TS packet aligns correctly
		    if ((i + this.PACKET_SIZE) < buffer.length && buffer[i + this.PACKET_SIZE] === this.SYNC_BYTE) {
			return i;
		    }
		}
	    }
	    return -1;
	}

    _handleDesync() {
        // If the buffer is too large without a sync byte, reset to avoid unbounded growth
        if (this.buffer.length > this.MAX_BUFFER_SIZE) {
            this.onLog('Buffer overflow without sync byte, resetting buffer.');
            this.buffer = new Uint8Array(0);
        }
    }

    _validatePackets(packets) {
        const validPackets = [];
        for (let i = 0; i < packets.length; i += this.PACKET_SIZE) {
            if (packets[i] === this.SYNC_BYTE) {
                validPackets.push(packets.slice(i, i + this.PACKET_SIZE));
            } else {
                this.onLog(`Skipping invalid packet at offset ${i} (expected 0x47, got 0x${packets[i].toString(16)})`);
            }
        }
        return validPackets.length > 0 ? validPackets : null;
    }
}

class WebTransportLoader extends BaseLoader {
    constructor() {
        super('webtransport-loader');
        this.TAG = 'WebTransportLoader';

        this._needStash = true;
        this._transport = null;
        this._reader = null;
        this._requestAbort = false;
        this._receivedLength = 0;
        this.tsBuffer = new MPEGTSBuffer((msg) => Log.v(this.TAG, msg));
        this.packetLogger = new PacketLogger((msg) => Log.v(this.TAG, msg));
    }

    static isSupported() {
        try {
            return typeof self.WebTransport !== 'undefined';
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

            Log.v(this.TAG, `Opening WebTransport connection to ${dataSource.url}`);

            this._transport = new WebTransport(dataSource.url);
            await this._transport.ready;

            const incomingStreams = this._transport.incomingUnidirectionalStreams;
            const streamReader = incomingStreams.getReader();
            const { value: stream } = await streamReader.read();
            if (!stream) {
                throw new Error('No incoming stream received');
            }

            this._reader = stream.getReader();
            this._status = LoaderStatus.kBuffering;

            this._readChunks();

        } catch (e) {
            this._status = LoaderStatus.kError;
            if (this._onError) {
                this._onError(LoaderErrors.EXCEPTION, { code: e.code || -1, msg: e.message });
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

	async _readChunks() {
	    try {
		let fragmentBuffer = new Uint8Array(0); // Store leftover data from previous chunk
		let syncRecovered = false; // Flag to track sync recovery state

		while (true) {
		    const { value, done } = await this._reader.read();
		    if (done || this._requestAbort) break;

		    if (value) {
			let chunk = value instanceof Uint8Array ? value : new Uint8Array(value);
			Log.v(this.TAG, `Received chunk of ${chunk.byteLength} bytes`);

			// 🛠 Merge any leftover bytes from previous chunk
			if (fragmentBuffer.length > 0) {
			    let merged = new Uint8Array(fragmentBuffer.length + chunk.length);
			    merged.set(fragmentBuffer, 0);
			    merged.set(chunk, fragmentBuffer.length);
			    chunk = merged;
			    fragmentBuffer = new Uint8Array(0); // Clear buffer
			}

			// 🔍 **Find a Valid MPEG-TS Sync Byte**
			let syncIndex = chunk.indexOf(0x47);
			if (syncIndex === -1) {
			    Log.w(this.TAG, `❌ No sync byte found in chunk, discarding ${chunk.byteLength} bytes`);
			    continue; // Drop corrupt chunk
			}

			// 🛠 **Realign MPEG-TS packets**
			let alignedData = chunk.slice(syncIndex);
			let validPackets = [];

			for (let i = 0; i + 188 <= alignedData.length; i += 188) {
			    if (alignedData[i] === 0x47) {
				validPackets.push(alignedData.slice(i, i + 188));
				syncRecovered = true; // Mark sync as recovered
			    } else if (syncRecovered) {
				Log.w(this.TAG, `⚠️ Lost sync at offset ${i}, attempting recovery`);
				let nextSync = alignedData.indexOf(0x47, i + 1);
				if (nextSync !== -1) {
				    i = nextSync - 188; // Jump to next valid packet
				}
			    }
			}

			// 🛠 **Store leftover bytes for the next chunk**
			let leftoverBytes = alignedData.length % 188;
			if (leftoverBytes > 0) {
			    fragmentBuffer = alignedData.slice(alignedData.length - leftoverBytes);
			    Log.v(this.TAG, `🔄 Buffered ${leftoverBytes} bytes for next chunk`);
			}

			// 🚀 **Process valid packets**
			if (validPackets.length > 0) {
			    const byteStart = this._receivedLength;
			    this._receivedLength += validPackets.length * 188;

			    validPackets.forEach(packet => this.packetLogger.logPacket(packet, Date.now()));

			    if (this._onDataArrival) {
				this._onDataArrival(new Uint8Array(validPackets.flat()), byteStart, this._receivedLength);
			    }
			}
		    }
		}
	    } catch (e) {
		this._status = LoaderStatus.kError;
		if (this._onError) {
		    this._onError(LoaderErrors.EXCEPTION, { code: e.code || -1, msg: e.message });
		}
	    }
	}

}

export default WebTransportLoader;
