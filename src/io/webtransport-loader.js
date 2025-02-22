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
        let syncIndex = this.findSyncByte(this.buffer);
        if (syncIndex === -1) {
            if (this.buffer.length > this.MAX_BUFFER_SIZE) {
                this.onLog('[MPEGTSBuffer] Buffer overflow, resetting');
                this.buffer = new Uint8Array(0);
            }
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

        return this.validatePackets(packetsData);
    }

    findSyncByte(buffer) {
        if (!buffer || buffer.length === 0) return -1;
        
        for (let i = 0; i <= buffer.length - this.PACKET_SIZE; i++) {
            if (buffer[i] === this.SYNC_BYTE) {
                // Check if the next MPEG-TS packet aligns correctly
                if ((i + this.PACKET_SIZE) < buffer.length && 
                    buffer[i + this.PACKET_SIZE] === this.SYNC_BYTE) {
                    return i;
                }
            }
        }
        return -1;
    }

    validatePackets(packets) {
        const validPackets = [];
        for (let i = 0; i < packets.length; i += this.PACKET_SIZE) {
            if (packets[i] === this.SYNC_BYTE) {
                validPackets.push(packets.slice(i, i + this.PACKET_SIZE));
            } else {
                this.onLog(`[MPEGTSBuffer] Skipping invalid packet at offset ${i}`);
            }
        }
        return validPackets.length > 0 ? validPackets : null;
    }
}

class PacketLogger {
    constructor(onLog) {
        this.packetCount = 0;
        this.onLog = onLog || (() => {});

        // PTS tracking & estimation
        this.lastValidPTS = null;
        this.prevPTS = null;
        this.estimatedFrameDuration = 3003;
        this.wraparoundOffset = 0;
        
        // Program info tracking
        this.videoPID = 256;
        this.lastVideoPTS = null;
        
        // Debug stats
        this.debugStats = {
            totalPackets: 0,
            videoPIDPackets: 0,
            validPTS: 0,
            notPESHeader: 0,
            noPTS: 0,
            pesStarts: 0,
            pts: []
        };
    }

    logPacket(packet, timeReceived) {
        if (!packet || !(packet instanceof Uint8Array) || packet.length !== 188) {
            return;
        }

        this.packetCount++;
        this.debugStats.totalPackets++;

        if (packet[0] !== 0x47) return;

        // Extract packet info
        const pid = ((packet[1] & 0x1F) << 8) | packet[2];
        const payloadUnitStart = (packet[1] & 0x40) !== 0;
        const hasAdaptationField = (packet[3] & 0x20) !== 0;
        const hasPayload = (packet[3] & 0x10) !== 0;

        if (pid === this.videoPID && hasPayload) {
            this.debugStats.videoPIDPackets++;
            
            // Calculate payload start
            let payloadOffset = 4;
            if (hasAdaptationField) {
                if (packet.length < 5) return;
                const adaptationFieldLength = packet[4];
                if (adaptationFieldLength > 183) return;
                payloadOffset = 5 + adaptationFieldLength;
            }

            if (payloadOffset >= packet.length) return;

            if (payloadUnitStart) {
                this.debugStats.pesStarts++;
                const payload = packet.slice(payloadOffset);
                
                // Debug full PES header details
                this._debugPESHeaderDetailed(payload);
                
                let pts = this._extractPTS(payload);
                if (pts !== null) {
                    this.debugStats.validPTS++;
                    pts = this._handleWraparound(pts);
                    this.lastValidPTS = pts;
                    this.lastVideoPTS = pts;
                    
                    this.debugStats.pts.push(pts);
                    if (this.debugStats.pts.length > 5) {
                        this.debugStats.pts.shift();
                    }
                    
                    this.onLog(`Found PTS: ${pts} in video packet #${this.debugStats.videoPIDPackets}`);
                }
            }
        }

        if (this.packetCount === 1 || this.packetCount === 100 || 
            this.packetCount === 1000 || this.packetCount % 1000 === 0) {
            this._logDetailedStats(timeReceived);
        }
    }

    _debugPESHeaderDetailed(payload) {
        if (payload.length < 19) return;

        if (payload[0] === 0x00 && payload[1] === 0x00 && payload[2] === 0x01) {
            const streamId = payload[3];
            const pesPacketLength = (payload[4] << 8) | payload[5];
            const scramblingControl = (payload[6] & 0x30) >> 4;
            const priority = (payload[6] & 0x08) >> 3;
            const dataAlignmentIndicator = (payload[6] & 0x04) >> 2;
            const copyright = (payload[6] & 0x02) >> 1;
            const originalOrCopy = payload[6] & 0x01;
            const ptsDtsFlags = (payload[7] & 0xC0) >> 6;
            const escrFlag = (payload[7] & 0x20) >> 5;
            const esRateFlag = (payload[7] & 0x10) >> 4;
            const dsmTrickModeFlag = (payload[7] & 0x08) >> 3;
            const additionalCopyInfoFlag = (payload[7] & 0x04) >> 2;
            const pesCrcFlag = (payload[7] & 0x02) >> 1;
            const pesExtensionFlag = payload[7] & 0x01;
            const pesHeaderLength = payload[8];

            this.onLog(`PES Header Detailed Debug:
                Start Code: ${payload[0].toString(16)},${payload[1].toString(16)},${payload[2].toString(16)}
                Stream ID: 0x${streamId.toString(16)}
                Packet Length: ${pesPacketLength}
                Scrambling Control: ${scramblingControl}
                Priority: ${priority}
                Data Alignment: ${dataAlignmentIndicator}
                Copyright: ${copyright}
                Original/Copy: ${originalOrCopy}
                PTS_DTS_flags: ${ptsDtsFlags}
                ESCR flag: ${escrFlag}
                ES Rate flag: ${esRateFlag}
                DSM Trick Mode: ${dsmTrickModeFlag}
                Additional Copy Info: ${additionalCopyInfoFlag}
                PES CRC flag: ${pesCrcFlag}
                PES Extension: ${pesExtensionFlag}
                PES Header Length: ${pesHeaderLength}
                Raw PTS bytes: ${payload.slice(9, 14).map(b => b.toString(16).padStart(2, '0')).join(' ')}
                PTS byte details:
                    Byte 1 (0x${payload[9].toString(16).padStart(2, '0')}): ${this._formatBits(payload[9])}
                    Byte 2 (0x${payload[10].toString(16).padStart(2, '0')}): ${this._formatBits(payload[10])}
                    Byte 3 (0x${payload[11].toString(16).padStart(2, '0')}): ${this._formatBits(payload[11])}
                    Byte 4 (0x${payload[12].toString(16).padStart(2, '0')}): ${this._formatBits(payload[12])}
                    Byte 5 (0x${payload[13].toString(16).padStart(2, '0')}): ${this._formatBits(payload[13])}`);
        }
    }

    _formatBits(byte) {
        return byte.toString(2).padStart(8, '0').match(/.{1,4}/g).join(' ');
    }

_extractPTS(payload) {
    // Validation
    if (payload.length < 14) return null;
    if (payload[0] !== 0x00 || payload[1] !== 0x00 || payload[2] !== 0x01) {
        this.debugStats.notPESHeader++;
        return null;
    }

    const streamId = payload[3];
    if (streamId < 0xE0 || streamId > 0xEF) return null;

    const ptsDtsFlags = (payload[7] & 0xC0) >> 6;
    if (ptsDtsFlags === 0) {
        this.debugStats.noPTS++;
        return null;
    }

    // Get PTS bytes
    const byte1 = payload[9];   // 0010 XXXX
    const byte2 = payload[10];  // XXXX XXXX
    const byte3 = payload[11];  // XXXX XXX1
    const byte4 = payload[12];  // XXXX XXXX
    const byte5 = payload[13];  // XXXX XXX1

    // Verify marker bits
    if ((byte1 & 0xF0) !== 0x20) {
        this.onLog(`Invalid PTS marker bits in byte1: ${this._formatBits(byte1)}`);
        return null;
    }
    if ((byte3 & 0x01) !== 0x01 || (byte5 & 0x01) !== 0x01) {
        this.onLog(`Invalid PTS marker bits in byte3/5: ${this._formatBits(byte3)}, ${this._formatBits(byte5)}`);
        return null;
    }

    // Calculate each byte's contribution
    const b1Contrib = ((byte1 & 0x0F) >>> 0) << 29;    // 4 bits from byte1 -> upper 4 bits
    const b2Contrib = (byte2 >>> 0) << 22;             // 8 bits from byte2
    const b3Contrib = ((byte3 & 0xFE) >>> 1) << 15;    // 7 bits from byte3 (ignore marker)
    const b4Contrib = (byte4 >>> 0) << 7;              // 8 bits from byte4
    const b5Contrib = (byte5 & 0xFE) >>> 1;            // 7 bits from byte5 (ignore marker)

    // Combine all parts
    const pts = b1Contrib | b2Contrib | b3Contrib | b4Contrib | b5Contrib;

    this.onLog(`PTS calculation:
            Raw bytes: ${[byte1, byte2, byte3, byte4, byte5].map(b => b.toString(16).padStart(2, '0')).join(' ')}
            Binary: ${[byte1, byte2, byte3, byte4, byte5].map(b => this._formatBits(b)).join(' ')}
            Byte1 contribution: ${b1Contrib.toString(16)}
            Byte2 contribution: ${b2Contrib.toString(16)}
            Byte3 contribution: ${b3Contrib.toString(16)}
            Byte4 contribution: ${b4Contrib.toString(16)}
            Byte5 contribution: ${b5Contrib.toString(16)}
            Final PTS: ${pts.toString(16)} (${pts})`);

    return pts;
}

    _handleWraparound(pts) {
        if (this.lastValidPTS !== null && pts < this.lastValidPTS) {
            const ptsDrop = this.lastValidPTS - pts;
            if (ptsDrop > 4294967296) {
                this.wraparoundOffset += 8589934592;
                this.onLog(`PTS wraparound detected! New offset: ${this.wraparoundOffset}`);
            }
            pts += this.wraparoundOffset;
        }
        return pts;
    }

    _logDetailedStats(timeReceived) {
        const pts = this.lastValidPTS !== null ? 
                   this.lastValidPTS : 
                   (this.packetCount * this.estimatedFrameDuration);

        this.onLog(`Packet Stats #${this.packetCount}:
            PTS: ${pts}
            Received at: ${timeReceived}
            Total Packets: ${this.debugStats.totalPackets}
            Video PID (256) Packets: ${this.debugStats.videoPIDPackets}
            PES Packet Starts: ${this.debugStats.pesStarts}
            Valid PTS Found: ${this.debugStats.validPTS}
            Not PES Header: ${this.debugStats.notPESHeader}
            No PTS: ${this.debugStats.noPTS}
            Last 5 PTS values: ${this.debugStats.pts.join(', ')}`);
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

        // Initialize with bound logging function
        const logFunction = (msg) => Log.v(this.TAG, msg);
        this.tsBuffer = new MPEGTSBuffer(logFunction);
        this.packetLogger = new PacketLogger(logFunction);

        // Bind methods
        this._readChunks = this._readChunks.bind(this);
        this._processPackets = this._processPackets.bind(this);
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

    _processPackets(packets) {
        if (!packets || !Array.isArray(packets)) return;

        const now = Date.now();
        packets.forEach(packet => {
            if (packet instanceof Uint8Array) {
                this._receivedLength += packet.length;
                this.packetLogger.logPacket(packet, now);

                if (this._onDataArrival) {
                    this._onDataArrival(packet, this._receivedLength - packet.length, this._receivedLength);
                }
            }
        });
    }

    async _readChunks() {
        try {
            let fragmentBuffer = new Uint8Array(0);

            while (true) {
                const { value, done } = await this._reader.read();
                if (done || this._requestAbort) break;

                if (value) {
                    let chunk = value instanceof Uint8Array ? value : new Uint8Array(value);
                    
                    // Only log if chunk size changed significantly
                    if (Math.abs(chunk.byteLength - this._lastChunkSize) > 100) {
                        Log.v(this.TAG, `Received chunk of ${chunk.byteLength} bytes`);
                        this._lastChunkSize = chunk.byteLength;
                    }

                    // Handle fragment from previous chunk
                    if (fragmentBuffer.length > 0) {
                        let merged = new Uint8Array(fragmentBuffer.length + chunk.length);
                        merged.set(fragmentBuffer, 0);
                        merged.set(chunk, fragmentBuffer.length);
                        chunk = merged;
                        fragmentBuffer = new Uint8Array(0);
                    }

                    const packets = this.tsBuffer.addChunk(chunk);
                    if (packets) {
                        this._processPackets(packets);
                    }
                }
            }
        } catch (e) {
            Log.e(this.TAG, `Error in _readChunks: ${e.message}`);
            this._status = LoaderStatus.kError;
            if (this._onError) {
                this._onError(LoaderErrors.EXCEPTION, { code: e.code || -1, msg: e.message });
            }
        }
    }
}

export default WebTransportLoader;

