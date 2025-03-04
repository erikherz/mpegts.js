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

        // Basic properties
        this._transport = null;
        this._streamReader = null;  // For reading incoming streams
        this._currentStreamReader = null;  // For reading current active stream
        this._requestAbort = false;
        this._receivedLength = 0;

        // Buffer for MPEG-TS packets
        this.PACKET_SIZE = 188;
        this.PACKETS_PER_CHUNK = 100;
        this._packetBuffer = [];

        // Diagnostic stats
        this.stats = {
            totalBytesReceived: 0,
            totalPacketsProcessed: 0,
            streamsReceived: 0,
            streamTasks: [],
            lastPTS: null
        };
    }

    static isSupported() {
        return typeof self.WebTransport !== 'undefined';
    }

    async open(dataSource) {
        try {
            Log.v(this.TAG, `Opening WebTransport connection to ${dataSource.url}`);

            // Create WebTransport connection
            this._transport = new WebTransport(dataSource.url);
            await this._transport.ready;

            // Set up error and close handlers
            this._transport.closed.then(info => {
                Log.v(this.TAG, `WebTransport connection closed: ${JSON.stringify(info)}`);
                if (this._status !== LoaderStatus.kComplete && this._status !== LoaderStatus.kError) {
                    this._status = LoaderStatus.kError;
                    if (this._onError) {
                        this._onError(LoaderErrors.CONNECTION_ERROR, {
                            code: -1,
                            msg: `WebTransport connection closed: ${JSON.stringify(info)}`
                        });
                    }
                }
            }).catch(error => {
                Log.e(this.TAG, `WebTransport connection error: ${error.message}`);
            });

            // Start processing incoming streams
            this._status = LoaderStatus.kBuffering;
            this._streamReader = this._transport.incomingUnidirectionalStreams.getReader();

            // Start processing streams
            this._processStreams();

            // Set up diagnostics timer if needed
            this.diagnosticTimer = setInterval(() => {
                this._logDiagnostics();
            }, 10000);

        } catch (e) {
            this._status = LoaderStatus.kError;
            if (this._onError) {
                this._onError(LoaderErrors.EXCEPTION, { code: e.code || -1, msg: e.message });
            }
        }
    }

    async _processStreams() {
        try {
            while (!this._requestAbort) {
                // Get the next stream
                const { value: stream, done } = await this._streamReader.read();

                if (done) {
                    Log.v(this.TAG, "No more incoming streams available");
                    break;
                }

                if (!stream) {
                    Log.w(this.TAG, "Received null stream, waiting for next stream");
                    continue;
                }

                this.stats.streamsReceived++;
                Log.v(this.TAG, `Received stream #${this.stats.streamsReceived}, processing...`);

                // Process this stream
                this._currentStreamReader = stream.getReader();

                // Create a task for this stream and don't wait for it
                const streamTask = this._readStreamData(this._currentStreamReader)
                    .catch(error => {
                        if (!this._requestAbort) {
                            Log.e(this.TAG, `Error reading stream: ${error.message}`);
                        }
                    });

                this.stats.streamTasks.push(streamTask);
            }

            Log.v(this.TAG, "Stream processing loop ended");

        } catch (e) {
            if (!this._requestAbort) {
                Log.e(this.TAG, `Error in _processStreams: ${e.message}`);
                this._status = LoaderStatus.kError;
                if (this._onError) {
                    this._onError(LoaderErrors.EXCEPTION, { code: e.code || -1, msg: e.message });
                }
            }
        }
    }

    async _readStreamData(reader) {
        let pendingData = new Uint8Array(0);

        try {
            while (!this._requestAbort) {
                const { value, done } = await reader.read();

                if (done) {
                    Log.v(this.TAG, "Stream ended, processing any remaining data");

                    // Process any remaining complete packets
                    if (pendingData.length >= this.PACKET_SIZE) {
                        const completePacketsLength = Math.floor(pendingData.length / this.PACKET_SIZE) * this.PACKET_SIZE;
                        this._processChunk(pendingData.slice(0, completePacketsLength));
                    }

                    // Force flush any remaining packets
                    if (this._packetBuffer.length > 0) {
                        this._dispatchPacketChunk();
                    }

                    break;
                }

                if (value) {
                    const chunk = value instanceof Uint8Array ? value : new Uint8Array(value);

                    // Update stats
                    this.stats.totalBytesReceived += chunk.length;
                    this._receivedLength += chunk.length;

                    // Combine with any pending data
                    let combinedChunk;
                    if (pendingData.length > 0) {
                        combinedChunk = new Uint8Array(pendingData.length + chunk.length);
                        combinedChunk.set(pendingData, 0);
                        combinedChunk.set(chunk, pendingData.length);
                    } else {
                        combinedChunk = chunk;
                    }

                    // Find sync byte alignment
                    const syncIndex = this._findSyncByteAlignment(combinedChunk);

                    if (syncIndex === -1) {
                        // No valid sync pattern found, store for next iteration
                        pendingData = combinedChunk;
                        continue;
                    } else if (syncIndex > 0) {
                        // Skip data before the first valid sync byte
                        combinedChunk = combinedChunk.slice(syncIndex);
                    }

                    // Extract complete packets
                    const completePacketsLength = Math.floor(combinedChunk.length / this.PACKET_SIZE) * this.PACKET_SIZE;

                    if (completePacketsLength > 0) {
                        // Process the aligned, complete packets
                        this._processChunk(combinedChunk.slice(0, completePacketsLength));
                    }

                    // Store any remaining bytes for next iteration
                    pendingData = combinedChunk.slice(completePacketsLength);
                }
            }

        } catch (e) {
            if (!this._requestAbort) {
                Log.e(this.TAG, `Error in _readStreamData: ${e.message}`);
                throw e;  // Re-throw to be caught by the caller
            }
        }
    }

    _findSyncByteAlignment(buffer) {
        // Try to find a pattern of sync bytes at PACKET_SIZE intervals
        for (let i = 0; i <= buffer.length - this.PACKET_SIZE * 2; i++) {
            if (buffer[i] === 0x47) {  // MPEG-TS sync byte
                // Check for multiple sync bytes at expected intervals
                let validSyncCount = 1;
                for (let j = 1; j <= 3; j++) {
                    const nextSyncPos = i + (j * this.PACKET_SIZE);
                    if (nextSyncPos < buffer.length && buffer[nextSyncPos] === 0x47) {
                        validSyncCount++;
                    } else {
                        break;
                    }
                }

                if (validSyncCount >= 2) {
                    return i;
                }
            }
        }

        // Fallback: return position of first sync byte
        for (let i = 0; i < buffer.length; i++) {
            if (buffer[i] === 0x47) {
                return i;
            }
        }

        return -1;  // No sync byte found
    }

    _processChunk(chunk) {
        if (!chunk || chunk.length === 0) return;

        // Extract and validate TS packets
        for (let i = 0; i < chunk.length; i += this.PACKET_SIZE) {
            if (i + this.PACKET_SIZE <= chunk.length) {
                const packet = chunk.slice(i, i + this.PACKET_SIZE);

                // Basic validation
                if (packet[0] === 0x47) {  // Check sync byte
                    this._packetBuffer.push(packet);
                    this.stats.totalPacketsProcessed++;

                    // Extract PTS for diagnostic purposes (simplified)
                    this._tryExtractPTS(packet);
                }
            }
        }

        // If we have enough packets, dispatch them
        if (this._packetBuffer.length >= this.PACKETS_PER_CHUNK) {
            this._dispatchPacketChunk();
        }
    }

    _dispatchPacketChunk() {
        if (this._packetBuffer.length === 0) return;

        // Combine packets into a single chunk
        const totalLength = this._packetBuffer.length * this.PACKET_SIZE;
        const chunk = new Uint8Array(totalLength);

        for (let i = 0; i < this._packetBuffer.length; i++) {
            chunk.set(this._packetBuffer[i], i * this.PACKET_SIZE);
        }

        // Send to demuxer
        if (this._onDataArrival) {
            this._onDataArrival(chunk.buffer, 0, totalLength);
        }

        // Clear buffer
        this._packetBuffer = [];
    }

    _tryExtractPTS(packet) {
        try {
            // This is a simplified PTS extraction for diagnostics
            // Real implementation should be more robust

            // Check if this is the start of PES
            const payloadStart = (packet[1] & 0x40) !== 0;
            const hasAdaptationField = (packet[3] & 0x20) !== 0;
            const hasPayload = (packet[3] & 0x10) !== 0;

            if (!payloadStart || !hasPayload) return null;

            // Calculate payload offset
            const payloadOffset = hasAdaptationField ?
                (5 + packet[4]) : 4;

            // Ensure we have enough bytes for a PES header
            if (packet.length < payloadOffset + 14) return null;

            // Check for PES start code
            if (packet[payloadOffset] !== 0x00 ||
                packet[payloadOffset + 1] !== 0x00 ||
                packet[payloadOffset + 2] !== 0x01) {
                return null;
            }

            // Check stream ID for video or audio
            const streamId = packet[payloadOffset + 3];
            if ((streamId < 0xE0 || streamId > 0xEF) && // Not video
                (streamId < 0xC0 || streamId > 0xDF)) { // Not audio
                return null;
            }

            // Check PTS_DTS_flags
            const ptsDtsFlags = (packet[payloadOffset + 7] & 0xC0) >> 6;
            if (ptsDtsFlags === 0) return null; // No PTS present

            // Extract PTS
            const p0 = packet[payloadOffset + 9];
            const p1 = packet[payloadOffset + 10];
            const p2 = packet[payloadOffset + 11];
            const p3 = packet[payloadOffset + 12];
            const p4 = packet[payloadOffset + 13];

            const pts = (((p0 >> 1) & 0x07) << 30) |
                       (p1 << 22) |
                       (((p2 >> 1) & 0x7F) << 15) |
                       (p3 << 7) |
                       ((p4 >> 1) & 0x7F);

            this.stats.lastPTS = pts;
            return pts;
        } catch (e) {
            return null;
        }
    }

    async abort() {
        this._requestAbort = true;

        // Clear diagnostic timer
        if (this.diagnosticTimer) {
            clearInterval(this.diagnosticTimer);
            this.diagnosticTimer = null;
        }

        try {
            // Cancel current stream reader if active
            if (this._currentStreamReader) {
                await this._currentStreamReader.cancel("Loader aborted").catch(() => {});
                this._currentStreamReader = null;
            }

            // Cancel stream reader
            if (this._streamReader) {
                await this._streamReader.cancel("Loader aborted").catch(() => {});
                this._streamReader = null;
            }

            // Dispatch any remaining packets
            if (this._packetBuffer.length > 0) {
                this._dispatchPacketChunk();
            }

            // Close transport
            if (this._transport && !this._transport.closed) {
                await this._transport.close().catch(() => {});
            }

        } catch (e) {
            Log.e(this.TAG, `Error during abort: ${e.message}`);
        } finally {
            this._transport = null;
            this._packetBuffer = [];
            this._status = LoaderStatus.kComplete;
        }
    }

    _logDiagnostics() {
        if (this._requestAbort) return;

        try {
            Log.v(this.TAG, "========= MPEG-TS STREAM DIAGNOSTIC REPORT =========");
            Log.v(this.TAG, `WebTransport State: ${this._transport ? (this._transport.closed ? 'closed' : 'open') : 'null'}`);
            Log.v(this.TAG, `Total Bytes Received: ${this._receivedLength}`);
            Log.v(this.TAG, `Total Packets Processed: ${this.stats.totalPacketsProcessed}`);
            Log.v(this.TAG, `Streams Received: ${this.stats.streamsReceived}`);
            Log.v(this.TAG, `Current Buffer: ${this._packetBuffer.length} packets`);

            if (this.stats.lastPTS !== null) {
                Log.v(this.TAG, `Last PTS value: ${this.stats.lastPTS}`);
            }

            Log.v(this.TAG, "====================================================");
        } catch (e) {
            Log.e(this.TAG, `Error in diagnostics: ${e.message}`);
        }
    }
}

export default WebTransportLoader;
