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

	    // 🛠 PTS tracking & estimation
	    this.lastValidPTS = null;  // Most recent valid PTS
	    this.prevPTS = null;       // PTS from the previous frame
	    this.estimatedFrameDuration = 3003; // Default for 30 FPS (90,000 / 30)
	    this.wraparoundOffset = 0; // Track wraparound corrections

	    // 🛠 Track how many times each PID was skipped
	    this.skipCountPerPID = {};
	}

    logPacket(packet, timeReceived) {
        this.packetCount++;
        let pts = this._extractPTS(packet);

        // 🔄 Handle missing PTS by estimating
        if (pts === null) {
            pts = this._estimatePTS();
            if (this.packetCount % 100 === 0) { // Reduce verbosity
                this.onLog(`[PacketLogger] ❗ Missing PTS, estimating: ${pts}`);
            }
        } else {
            pts = this._handleWraparound(pts);
            this._checkPTSOrdering(pts);
            this.lastValidPTS = pts;
        }

        // 📌 Log at key packet intervals
        if (this.packetCount === 1 || this.packetCount === 100 || this.packetCount === 1000 || this.packetCount % 1000 === 0) {
            this.onLog(`[PacketLogger] #${this.packetCount} | PTS: ${pts} | Received at: ${timeReceived}`);
        }
    }

	_extractPTS(packet) {
	    if (packet.length < 19) {
		this.onLog(`[PacketLogger] ❗ Packet too short for PTS extraction`);
		return null;
	    }

	    // ✅ Verify sync byte
	    if (packet[0] !== 0x47) {
		this.onLog(`[PacketLogger] ❗ Invalid TS packet, missing sync byte`);
		return null;
	    }

	    // ✅ Extract PID
	    const pid = ((packet[1] & 0x1F) << 8) | packet[2];

	    // ✅ Ignore NULL packets
	    if (pid === 0x1FFF) return null;

	    // ✅ Check for payload
	    const adaptationFieldControl = (packet[3] & 0x30) >> 4;
	    if (adaptationFieldControl === 0x00 || adaptationFieldControl === 0x02) return null;

	    let payloadStart = 4;
	    if (adaptationFieldControl === 0x03) payloadStart += 1 + packet[4];

	    // ✅ Ensure it's a PES header
	    if (packet[payloadStart] !== 0x00 || packet[payloadStart + 1] !== 0x00 || packet[payloadStart + 2] !== 0x01) {
		this.skipCountPerPID[pid] = (this.skipCountPerPID[pid] || 0) + 1;
		if (this.skipCountPerPID[pid] <= 3) {  // Log only first 3 occurrences per PID
		    this.onLog(`[PacketLogger] ❗ Skipping PID ${pid} - Not a PES header`);
		}
		return null;
	    }

	    // ✅ Extract PTS if present
	    const ptsDtsFlags = (packet[payloadStart + 7] & 0xC0) >> 6;
	    if (ptsDtsFlags === 0) {
		this.onLog(`[PacketLogger] ❗ No PTS in PID ${pid}`);
		return null;
	    }

	    const ptsOffset = payloadStart + 9;
	    return ((packet[ptsOffset] & 0x0E) << 29) |
		   ((packet[ptsOffset + 1] & 0xFF) << 22) |
		   ((packet[ptsOffset + 2] & 0xFE) << 14) |
		   ((packet[ptsOffset + 3] & 0xFF) << 7) |
		   ((packet[ptsOffset + 4] & 0xFE) >> 1);
	}

	_handleWraparound(pts) {
	    if (this.lastValidPTS !== null && pts < this.lastValidPTS) {
		const ptsDrop = this.lastValidPTS - pts;

		// Only adjust for wraparound if drop exceeds 4GB (approx 47 minutes)
		if (ptsDrop > 4294967296) { 
		    this.wraparoundOffset += 8589934592;
		    this.onLog(`[PacketLogger] ⚠️ PTS wraparound detected! New offset: ${this.wraparoundOffset}`);
		} else {
		    this.onLog(`[PacketLogger] ❓ Small PTS drop (${ptsDrop}), not a wraparound.`);
		}

		pts += this.wraparoundOffset;
	    }
	    return pts;
	}


    _checkPTSOrdering(currentPTS) {
        if (this.lastValidPTS !== null && currentPTS < this.lastValidPTS - 90000) {
            this.onLog(`[PacketLogger] ⚠️ Possible PTS disorder: ${currentPTS} < ${this.lastValidPTS}`);
        }
    }

	_estimatePTS() {
	    if (this.lastValidPTS === null) {
		return 90000; // Start from a reasonable default (1 second @ 90kHz)
	    }

	    // If previous PTS is same as lastValidPTS, avoid recalculating
	    if (this.prevPTS === this.lastValidPTS) {
		return this.lastValidPTS + this.estimatedFrameDuration;
	    }

	    const diff = this.lastValidPTS - this.prevPTS;

	    // Ignore outliers, but allow natural frame durations
	    if (diff > 500 && diff < 50000) {  
		this.estimatedFrameDuration = diff;
	    } else if (diff !== 0) {
		this.onLog(`[PacketLogger] ⚠️ Unusual frame duration: ${diff}, ignoring.`);
	    }

	    this.prevPTS = this.lastValidPTS;
	    this.lastValidPTS += this.estimatedFrameDuration;
	    return this.lastValidPTS;
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
	    let syncIndex = this.tsBuffer._findSyncByte(this.buffer);
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
		let fragmentBuffer = new Uint8Array(0);
		const TS_PACKET_SIZE = 188;
		const SYNC_BYTE = 0x47;
		let firstChunkProcessed = false;

		while (true) {
		    const { value, done } = await this._reader.read();
		    if (done || this._requestAbort) break;

		    if (value) {
			let chunk = value instanceof Uint8Array ? value : new Uint8Array(value);
			Log.v(this.TAG, `Received chunk of ${chunk.byteLength} bytes`);

			// ✅ Always prepend leftover bytes from the previous chunk
			if (fragmentBuffer.length > 0) {
			    let merged = new Uint8Array(fragmentBuffer.length + chunk.length);
			    merged.set(fragmentBuffer, 0);
			    merged.set(chunk, fragmentBuffer.length);
			    chunk = merged;
			    fragmentBuffer = new Uint8Array(0);
			}

			let offset = 0;
			let totalLength = chunk.length;

			// ✅ Handle first chunk separately
			if (!firstChunkProcessed) {
			    let syncPos = chunk.indexOf(SYNC_BYTE);
			    if (syncPos === -1) {
				Log.w(this.TAG, `⚠️ No sync byte found in the first chunk, discarding entire chunk`);
				continue;  // Skip this chunk and wait for the next one
			    }
			    if (syncPos > 0) {
				Log.w(this.TAG, `⚠️ Skipping ${syncPos} bytes before first sync`);
				chunk = chunk.slice(syncPos);
				totalLength = chunk.length;
			    }
			    firstChunkProcessed = true;
			}

			let numPackets = Math.floor(totalLength / TS_PACKET_SIZE);
			let leftoverBytes = totalLength % TS_PACKET_SIZE;

			// ✅ Extract and forward all full 188-byte packets
			for (let i = 0; i < numPackets; i++) {
			    let packetStart = i * TS_PACKET_SIZE;
			    let packet = chunk.slice(packetStart, packetStart + TS_PACKET_SIZE);

			    if (packet[0] === SYNC_BYTE) {
				//this.packetLogger.logPacket(packet, Date.now());
				if (this._onDataArrival) {
				    this._onDataArrival(packet, this._receivedLength, this._receivedLength + TS_PACKET_SIZE);
				}
				this._receivedLength += TS_PACKET_SIZE;
			    } else {
				Log.e(this.TAG, `[TSDemuxer] ❗ Sync byte missing at packet boundary (expected 0x47, got ${packet[0]})`);
			    }
			}

			// ✅ Always store any remaining bytes for the next chunk
			if (leftoverBytes > 0) {
			    let tailData = chunk.slice(numPackets * TS_PACKET_SIZE);
			    if (tailData[0] === SYNC_BYTE) {
				fragmentBuffer = new Uint8Array(0);  // Clear it first
				fragmentBuffer = tailData;  // Then store new fragment
				Log.v(this.TAG, `🔄 Buffered ${leftoverBytes} bytes for next chunk`);
			    } else {
				Log.w(this.TAG, `⚠️ Dropping ${leftoverBytes} bytes (not part of a valid TS packet)`);
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

