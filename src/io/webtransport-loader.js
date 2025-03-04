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

/**
 * PTSContinuityHandler - Manages PTS continuity across WebTransport stream boundaries
 *
 * This lightweight handler tracks PTS values across stream transitions and
 * applies adjustments to maintain monotonically increasing timestamps.
 */
class PTSContinuityHandler {
    constructor() {
        this.TAG = 'PTSContinuityHandler';

        // Track last PTS for each stream type
        this._lastPTS = {
            video: null,  // Last video PTS
            audio: null   // Last audio PTS
        };

        // PTS adjustment offsets for each stream type
        this._ptsOffsets = {
            video: 0,
            audio: 0
        };

        // Stream transition detection
        this._streamTransitionDetected = false;
        this._newStreamStarted = false;

        // Typical frame durations (90kHz clock)
        this.TYPICAL_VIDEO_FRAME_DURATION = 3000;  // ~33ms at 30fps
        this.TYPICAL_AUDIO_FRAME_DURATION = 1536;  // ~AAC frame duration

        // PTS wraparound handling (33-bit PTS)
        this.PTS_MAX_VALUE = 8589934591;  // 2^33 - 1
        this.PTS_WRAPAROUND_THRESHOLD = 8589934591 - 90000 * 10;  // 10 seconds before wraparound

        // Continuity reset threshold (ms)
        this.CONTINUITY_RESET_THRESHOLD = 5000;  // 5 seconds
        this._lastStreamTransitionTime = Date.now();

        // Enable detailed logging
        this.enableDetailedLogging = false;
    }

    /**
     * Notifies the handler of a stream transition
     */
    notifyStreamTransition() {
        if (this.enableDetailedLogging) {
            Log.v(this.TAG, `Stream transition detected`);
        }

        this._streamTransitionDetected = true;
        this._newStreamStarted = true;
        this._lastStreamTransitionTime = Date.now();
    }

    /**
     * Process a PTS value for a specific stream type, applying continuity adjustments
     *
     * @param {number} pts The original PTS value
     * @param {string} streamType Either 'video' or 'audio'
     * @return {number} The adjusted PTS value ensuring continuity
     */
    processPTS(pts, streamType) {
        if (pts === null || pts === undefined) {
            return null;
        }

        // Handle potential stream boundary conditions
        if (this._newStreamStarted && this._lastPTS[streamType] !== null) {
            this._calculatePTSOffset(pts, streamType);
            this._newStreamStarted = false;
        }

        // Apply the offset
        const adjustedPTS = this._applyOffset(pts, streamType);

        // Track the last adjusted PTS
        this._lastPTS[streamType] = adjustedPTS;

        return adjustedPTS;
    }

	/**
	 * Calculate the PTS offset to maintain continuity when transitioning to a new stream
	 * 
	 * @param {number} firstPTS The first PTS value in the new stream
	 * @param {string} streamType The stream type ('video' or 'audio')
	 */
	_calculatePTSOffset(firstPTS, streamType) {
	    // We only calculate offset based on the reference stream (video)
	    // and then apply the same offset to all streams
	    if (streamType !== 'video') {
		return;
	    }
	    
	    const lastPTS = this._lastPTS[streamType];
	    
	    if (lastPTS === null) {
		return;
	    }
	    
	    // Expected next PTS based on typical frame duration
	    const frameDuration = this.TYPICAL_VIDEO_FRAME_DURATION;
	    
	    let expectedPTS = lastPTS + frameDuration;
	    
	    // Handle PTS wraparound
	    if (expectedPTS > this.PTS_MAX_VALUE) {
		expectedPTS -= this.PTS_MAX_VALUE + 1;
	    }
	    
	    // Calculate offset needed (accounting for wraparound conditions)
	    let offset = 0;
	    
	    if (firstPTS < expectedPTS && (expectedPTS - firstPTS) > this.PTS_MAX_VALUE / 2) {
		// PTS wraparound case: first PTS from new stream is after wraparound
		offset = (expectedPTS - this.PTS_MAX_VALUE - 1) - firstPTS;
	    } else if (firstPTS > expectedPTS && (firstPTS - expectedPTS) > this.PTS_MAX_VALUE / 2) {
		// PTS wraparound case: last PTS from old stream is near wraparound
		offset = (expectedPTS + this.PTS_MAX_VALUE + 1) - firstPTS;
	    } else {
		// Normal case: no wraparound
		offset = expectedPTS - firstPTS;
	    }
	    
	    // Update the unified offset for all stream types
	    // This is the key change - we apply the same offset to all streams
	    const unifiedOffset = offset;
	    for (const type in this._ptsOffsets) {
		this._ptsOffsets[type] += unifiedOffset;
	    }
	    
	    if (this.enableDetailedLogging) {
		Log.v(this.TAG, `Calculated unified PTS offset: ${unifiedOffset}`);
		Log.v(this.TAG, `Applied to all streams - video: ${this._ptsOffsets.video}, audio: ${this._ptsOffsets.audio}`);
	    }
	}

    /**
     * Apply the calculated offset to a PTS value
     *
     * @param {number} pts Original PTS value
     * @param {string} streamType The stream type ('video' or 'audio')
     * @return {number} Adjusted PTS value
     */
    _applyOffset(pts, streamType) {
        let adjustedPTS = pts + this._ptsOffsets[streamType];

        // Handle wraparound for adjusted PTS
        if (adjustedPTS > this.PTS_MAX_VALUE) {
            adjustedPTS -= this.PTS_MAX_VALUE + 1;
        } else if (adjustedPTS < 0) {
            adjustedPTS += this.PTS_MAX_VALUE + 1;
        }

        return adjustedPTS;
    }

    /**
     * Check if PTS value is approaching wraparound
     *
     * @param {number} pts The PTS value to check
     * @return {boolean} True if PTS is approaching wraparound
     */
    isApproachingWraparound(pts) {
        return pts > this.PTS_WRAPAROUND_THRESHOLD;
    }

    /**
     * Reset continuity tracking
     * This should be called when a full reset is needed (e.g., seek operation)
     */
    reset() {
        this._lastPTS = {
            video: null,
            audio: null
        };

        this._ptsOffsets = {
            video: 0,
            audio: 0
        };

        this._streamTransitionDetected = false;
        this._newStreamStarted = false;
        this._lastStreamTransitionTime = Date.now();

        Log.v(this.TAG, `PTS continuity tracking reset`);
    }

    /**
     * Check for conditions that would require a reset of continuity tracking
     * Should be called periodically
     */
    checkForResetConditions() {
        const now = Date.now();
        const timeSinceLastTransition = now - this._lastStreamTransitionTime;

        // Reset if it's been a long time since the last transition
        if (timeSinceLastTransition > this.CONTINUITY_RESET_THRESHOLD) {
            if (this._streamTransitionDetected) {
                this._streamTransitionDetected = false;

                if (this.enableDetailedLogging) {
                    Log.v(this.TAG, `Stream transition resolved, continuity maintained`);
                }
            }
        }
    }

    /**
     * Extract PTS from a TS packet
     *
     * @param {Uint8Array} packet The MPEG-TS packet
     * @return {Object|null} Object containing PTS and stream type, or null if not found
     */
    extractPTSInfo(packet) {
        try {
            // Check if this is the start of PES
            const payloadStart = (packet[1] & 0x40) !== 0;
            const hasAdaptationField = (packet[3] & 0x20) !== 0;
            const hasPayload = (packet[3] & 0x10) !== 0;

            if (!payloadStart || !hasPayload) return null;

            // Calculate payload offset
            const adaptationFieldLength = hasAdaptationField ? packet[4] : 0;
            const payloadOffset = hasAdaptationField ? 5 + adaptationFieldLength : 4;

            // Ensure we have enough bytes for a PES header
            if (packet.length < payloadOffset + 14) return null;

            // Check for PES start code
            if (packet[payloadOffset] !== 0x00 ||
                packet[payloadOffset + 1] !== 0x00 ||
                packet[payloadOffset + 2] !== 0x01) {
                return null;
            }

            // Check stream ID to determine type
            const streamId = packet[payloadOffset + 3];
            let streamType = null;

            if (streamId >= 0xE0 && streamId <= 0xEF) {
                streamType = 'video';
            } else if (streamId >= 0xC0 && streamId <= 0xDF) {
                streamType = 'audio';
            } else {
                return null; // Not a video or audio stream
            }

            // Check PTS_DTS_flags
            const ptsDtsFlags = (packet[payloadOffset + 7] & 0xC0) >> 6;
            if (ptsDtsFlags === 0) return null; // No PTS present

            // Extract PTS
            const ptsOffset = payloadOffset + 9;
            const p0 = packet[ptsOffset];
            const p1 = packet[ptsOffset + 1];
            const p2 = packet[ptsOffset + 2];
            const p3 = packet[ptsOffset + 3];
            const p4 = packet[ptsOffset + 4];

            const pts = (((p0 & 0x0E) << 29) |
                         ((p1 & 0xFF) << 22) |
                         ((p2 & 0xFE) << 14) |
                         ((p3 & 0xFF) << 7) |
                         ((p4 & 0xFE) >> 1));

            return {
                pts: pts,
                streamType: streamType
            };

        } catch (e) {
            return null;
        }
    }
}

/**
 * WebTransportLoader - Loads MPEG-TS data over WebTransport connection
 * Enhanced with PTS continuity maintenance across stream boundaries
 */
class WebTransportLoader extends BaseLoader {
    constructor() {
        super('webtransport-loader');
        this.TAG = 'WebTransportLoader';

        this._needStash = true;
        this._transport = null;
        this._streamReader = null;  // For reading incoming streams
        this._currentStreamReader = null;  // For reading current active stream
        this._requestAbort = false;
        this._receivedLength = 0;

        // Configure settings
        this.config = {
            enableDetailedLogging: false,
            packetFlushThreshold: 50,  // Number of packets per chunk
            enablePTSContinuity: true   // Enable PTS continuity handling
        };

        // Packet buffering
        this.PACKET_SIZE = 188;
        this._packetBuffer = [];

        // Diagnostics
        this.stats = {
            totalBytesReceived: 0,
            totalPacketsProcessed: 0,
            streamsReceived: 0,
            lastPTS: {
                video: null,
                audio: null
            },
            ptsAdjustments: {
                video: 0,
                audio: 0
            }
        };
        
        // Initialize PTS continuity handler
        this._ptsHandler = new PTSContinuityHandler();
        this._ptsHandler.enableDetailedLogging = this.config.enableDetailedLogging;
    }

    static isSupported() {
        try {
            return typeof self.WebTransport !== 'undefined';
        } catch (e) {
            return false;
        }
    }

    async open(dataSource) {
        try {
            if (!dataSource.url.startsWith('https://')) {
                throw new Error('WebTransport requires HTTPS URL');
            }

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
                
                // Check for PTS continuity reset conditions
                if (this.config.enablePTSContinuity) {
                    this._ptsHandler.checkForResetConditions();
                }
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
                
                // Notify PTS handler about stream transition if enabled
                if (this.config.enablePTSContinuity) {
                    this._ptsHandler.notifyStreamTransition();
                }

                // Process this stream until it's done
                this._currentStreamReader = stream.getReader();
                await this._readStreamData(this._currentStreamReader);
            }

            // Final flush of any remaining packets
            if (this._packetBuffer.length > 0 && !this._requestAbort) {
                this._dispatchPacketChunk();
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

async _readStreamData(reader) {
    let pendingData = new Uint8Array(0);

    try {
        while (!this._requestAbort) {
            const { value, done } = await reader.read();

            if (done) {
                Log.v(this.TAG, "Stream ended, processing any remaining data");

                // Process any remaining complete packets without forcing a flush
                if (pendingData.length >= this.PACKET_SIZE) {
                    const completePacketsLength = Math.floor(pendingData.length / this.PACKET_SIZE) * this.PACKET_SIZE;
                    this._processChunk(pendingData.slice(0, completePacketsLength));
                }

                // Note: We're intentionally NOT flushing remaining packets here
                // to maintain buffer continuity across stream transitions

                return; // Stream is done
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
        }
    }
}

_processChunk(chunk) {
    if (!chunk || chunk.length === 0) return;

    // Extract and validate TS packets
    for (let i = 0; i < chunk.length; i += this.PACKET_SIZE) {
        if (i + this.PACKET_SIZE <= chunk.length) {
            const packet = chunk.slice(i, i + this.PACKET_SIZE);

            // Basic validation - check sync byte
            if (packet[0] === 0x47) {
                // Process PTS information if continuity handling is enabled
                if (this.config.enablePTSContinuity) {
                    this._processPTSInPacket(packet);
                }
                
                this._packetBuffer.push(packet);
                this.stats.totalPacketsProcessed++;
            }
        }
    }

    // If we have enough packets, dispatch them
    while (this._packetBuffer.length >= this.config.packetFlushThreshold) {
        // Create a chunk with exactly packetFlushThreshold packets
        const packetsToDispatch = this._packetBuffer.slice(0, this.config.packetFlushThreshold);
        
        // Remove those packets from the buffer
        this._packetBuffer = this._packetBuffer.slice(this.config.packetFlushThreshold);
        
        // Combine packets into a single chunk
        const totalLength = packetsToDispatch.reduce((sum, packet) => sum + packet.length, 0);
        const chunk = new Uint8Array(totalLength);
        
        let offset = 0;
        packetsToDispatch.forEach(packet => {
            chunk.set(packet, offset);
            offset += packet.length;
        });
        
        // Send to demuxer
        if (this._onDataArrival) {
            this._onDataArrival(chunk.buffer, 0, totalLength);
        }
    }
}

	/**
	 * Process PTS information in a packet
	 * 
	 * @param {Uint8Array} packet MPEG-TS packet
	 */
	_processPTSInPacket(packet) {
	    const ptsInfo = this._ptsHandler.extractPTSInfo(packet);
	    
	    if (ptsInfo) {
		// Process the PTS value through the continuity handler
		const adjustedPTS = this._ptsHandler.processPTS(ptsInfo.pts, ptsInfo.streamType);
		
		// Update stats
		if (adjustedPTS !== null) {
		    this.stats.lastPTS[ptsInfo.streamType] = adjustedPTS;
		    // Now all stream types have the same adjustment value
		    this.stats.ptsAdjustments[ptsInfo.streamType] = this._ptsHandler._ptsOffsets[ptsInfo.streamType];
		}
	    }
	}
    
	/**
	 * Process a PTS value for a specific stream type, applying continuity adjustments
	 * 
	 * @param {number} pts The original PTS value
	 * @param {string} streamType Either 'video' or 'audio'
	 * @return {number} The adjusted PTS value ensuring continuity
	 */
	processPTS(pts, streamType) {
	    if (pts === null || pts === undefined) {
		return null;
	    }
	    
	    // Handle potential stream boundary conditions
	    // Only video stream will trigger offset calculation, but
	    // the resulting offset will be applied to all streams
	    if (this._newStreamStarted && this._lastPTS.video !== null && streamType === 'video') {
		this._calculatePTSOffset(pts, streamType);
		this._newStreamStarted = false;
	    }
	    
	    // Apply the unified offset (same for all stream types)
	    const adjustedPTS = this._applyOffset(pts, streamType);
	    
	    // Track the last adjusted PTS
	    this._lastPTS[streamType] = adjustedPTS;
	    
	    return adjustedPTS;
	}

    _dispatchPacketChunk() {
        if (this._packetBuffer.length === 0) return;

        // Combine packets into a single chunk
        const totalLength = this._packetBuffer.reduce((sum, packet) => sum + packet.length, 0);
        const chunk = new Uint8Array(totalLength);

        let offset = 0;
        this._packetBuffer.forEach(packet => {
            chunk.set(packet, offset);
            offset += packet.length;
        });

        // Send to demuxer
        if (this._onDataArrival) {
            this._onDataArrival(chunk.buffer, 0, totalLength);
        }

        // Clear buffer
        this._packetBuffer = [];
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

	/**
	 * Log diagnostics information about the stream and PTS continuity
	 */
	_logDiagnostics() {
	    if (this._requestAbort) return;

	    try {
		Log.v(this.TAG, "========= MPEG-TS STREAM DIAGNOSTIC REPORT =========");
		Log.v(this.TAG, `WebTransport State: ${this._transport ? (this._transport.closed ? 'closed' : 'open') : 'null'}`);
		Log.v(this.TAG, `Total Bytes Received: ${this._receivedLength}`);
		Log.v(this.TAG, `Total Packets Processed: ${this.stats.totalPacketsProcessed}`);
		Log.v(this.TAG, `Streams Received: ${this.stats.streamsReceived}`);
		Log.v(this.TAG, `Current Buffer: ${this._packetBuffer.length} packets`);

		if (this.config.enablePTSContinuity) {
		    Log.v(this.TAG, `PTS Continuity: ENABLED`);
		    Log.v(this.TAG, `Last Video PTS: ${this.stats.lastPTS.video !== null ? this.stats.lastPTS.video : 'None'}`);
		    Log.v(this.TAG, `Last Audio PTS: ${this.stats.lastPTS.audio !== null ? this.stats.lastPTS.audio : 'None'}`);
		    // Highlight that adjustments are now unified
		    const unifiedAdjustment = this.stats.ptsAdjustments.video;
		    Log.v(this.TAG, `Unified PTS Adjustment: ${unifiedAdjustment} (applied to all streams)`);
		} else {
		    Log.v(this.TAG, `PTS Continuity: DISABLED`);
		}

		Log.v(this.TAG, "====================================================");
	    } catch (e) {
		Log.e(this.TAG, `Error in diagnostics: ${e.message}`);
	    }
	}

    /**
     * Reset timestamp continuity tracking
     * This should be called during seek operations
     */
    resetPTSContinuity() {
        if (this.config.enablePTSContinuity && this._ptsHandler) {
            this._ptsHandler.reset();
            Log.v(this.TAG, "PTS continuity tracking has been reset");
            
            // Reset stats
            this.stats.lastPTS = {
                video: null,
                audio: null
            };
            this.stats.ptsAdjustments = {
                video: 0,
                audio: 0
            };
        }
    }
}

export default WebTransportLoader;
